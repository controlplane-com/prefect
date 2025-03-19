import asyncio
import copy
import json
import logging
import os
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
from urllib.parse import urlencode

import anyio.abc
import requests
import websockets
import yaml
from tenacity import retry, stop_after_attempt, wait_fixed, wait_random
from typing_extensions import Literal

from prefect._internal.pydantic import HAS_PYDANTIC_V2
from prefect._internal.schemas.validators import (
    cast_k8s_job_customizations,
    set_default_image,
    validate_k8s_job_compatible_values,
    validate_k8s_job_required_components,
)
from prefect.blocks.cpln import CplnClient, CplnInfrastructureConfig
from prefect.exceptions import (
    InfrastructureError,
    InfrastructureNotAvailable,
    InfrastructureNotFound,
)
from prefect.infrastructure.base import Infrastructure, InfrastructureResult
from prefect.utilities.asyncutils import sync_compatible
from prefect.utilities.hashing import stable_hash
from prefect.utilities.pydantic import JsonPatch
from prefect.utilities.slugify import slugify

if HAS_PYDANTIC_V2:
    from pydantic.v1 import Field, root_validator, validator
else:
    from pydantic import Field, root_validator, validator

### Constants ###

# Jobs Related
COMMAND_STATUS_CHECK_DELAY: int = 5
JOB_WATCH_OFFSET_MINUTES: int = 5
LIFECYCLE_FINAL_STAGES: List[str] = ["completed", "failed", "cancelled"]

# Retry Related
RETRY_MAX_ATTEMPTS = 3
RETRY_MIN_DELAY_SECONDS = 1
RETRY_MIN_DELAY_JITTER_SECONDS = 0
RETRY_MAX_DELAY_JITTER_SECONDS = 3
RETRY_WORKLOAD_READY_CHECK_SECONDS = 2

# API Request Configuration
HTTP_REQUEST_TIMEOUT: int = 60
HTTP_REQUEST_RETRY_BASE_DELAY: int = 1
HTTP_REQUEST_MAX_RETRIES: int = 5
WEB_SOCKET_PING_INTERVAL_MS = 30 * 1000

# API Endpoints
DEFAULT_DATA_SERVICE_URL: str = "https://api.cpln.io"
LOGS_URL: str = "wss://logs.cpln.io"

# Object Structure Definitions
CplnObjectManifest = Dict[str, Any]
KubernetesObjectManifest = Dict[str, Any]

### Classes ###


class CplnLogsMonitor:
    """Allows you to monitor logs of a running job."""

    def __init__(
        self,
        logger: logging.Logger,
        client: CplnClient,
        org: str,
        gvc: str,
        location: str,
        workload_name: str,
        command_id: str,
        interval_seconds: Optional[int] = None,
    ):
        # Received attributes
        self._logger = logger
        self._client = client
        self._org = org
        self._gvc = gvc
        self._location = location
        self._workload_name = workload_name
        self._command_id = command_id
        self._interval_seconds = interval_seconds
        self.logs_url = os.getenv("CPLN_LOGS_ENDPOINT", LOGS_URL)

        # Defined attributes
        self.completed = False
        self.job_status = None
        self._websocket = None

    ### Public Methods ###

    async def monitor(self, print_func: Optional[Callable] = None):
        """
        Monitor the job status and logs.

        Args:
            print_func (Optional[Callable]): If provided, it will stream the logs by calling `print_func`
            for every log message received from the logs service.
        """

        # Define tasks
        monitor_status_task = asyncio.create_task(self._monitor_job_status())
        monitor_logs_task = asyncio.create_task(self._monitor_job_logs(print_func))

        # Store tasks into a list
        tasks = [monitor_status_task, monitor_logs_task]

        # Log a message indicating status check and logs monitoring will start soon
        self._logger.info("Starting status check and logs monitoring...")

        # Wait for either task to complete
        _, pending = await asyncio.wait(
            tasks,
            return_when=asyncio.FIRST_COMPLETED,
        )

        # Ensure WebSocket disconnects if the status monitoring triggers disconnection
        if self._websocket and not self._websocket.closed:
            # Close the WebSocket connection
            await self._websocket.close()

        # Cancel remaining tasks
        for task in pending:
            task.cancel()

    ### Private Methods ###

    async def _monitor_job_status(self):
        """
        Fetch the job status from the API and check if it has completed.
        Sets `self.completed` to True if the job has completed, failed, or cancelled.
        """

        # Watch the job until its completion
        self.job_status = await watch_job_until_completion(
            self._client,
            self._org,
            self._gvc,
            self._workload_name,
            self._command_id,
            self._interval_seconds,
        )

        # If the job completed, then we should get here
        self.completed = True

    async def _monitor_job_logs(self, print_func: Optional[Callable] = None):
        """
        Connect to the logs service via a WebSocket connection and handle incoming messages until job completes.

        Args:
            print_fund (Optional[Callable]): If provided, it will stream the logs by calling `print_func`
            for every log message received from the logs service.
        """

        # Keep retrying to monitor logs until the job completes
        while not self.completed:
            try:
                # Current time in milliseconds
                now_millis = int(time.time() * 1000)

                # Subtract the offset in milliseconds
                offset_millis = JOB_WATCH_OFFSET_MINUTES * 60 * 1000
                offset_time_millis = now_millis - offset_millis

                # Convert to nanoseconds
                nanos = f"{offset_time_millis * 1_000_000}"

                # Construct the query parameters
                query_params = {
                    "query": f'{{gvc="{self._gvc}", location="{self._location}", workload="{self._workload_name}", replica=~"command-{self._command_id}.+"}}',
                    "start": nanos,
                    "direction": "forward",
                    "limit": 1000,
                }

                # Construct the WebSocket URL with the query parameters
                url = f"{self.logs_url}/logs/org/{self._org}/loki/api/v1/tail?{urlencode(query_params)}"

                # Set the authorization token in the headers
                headers = {"Authorization": self._client.token}

                # Connect to the WebSocket using the WebSocket URL and headers
                async with websockets.connect(
                    url,
                    extra_headers=headers,
                    ping_interval=WEB_SOCKET_PING_INTERVAL_MS,
                ) as websocket:
                    # Set the WebSocket connection
                    self._websocket = websocket
                    # Continue receiving messages until the job completes
                    while not self.completed:
                        # Receive a message from the WebSocket
                        message = await websocket.recv()

                        # Parse the message as JSON
                        obj = json.loads(message)

                        # Extract the streams from the message
                        for stream in obj.get("streams", []):
                            # Extract the values from the stream
                            for value in stream.get("values", []):
                                # Extract the timestamp and log message from the value
                                timestamp_ns = int(value[0])
                                log_message = value[1]

                                # Convert timestamp from nanoseconds to a readable format
                                timestamp = datetime.fromtimestamp(
                                    timestamp_ns / 1_000_000_000
                                )

                                # Call the print_func and send the timestamp and log message if specified
                                if print_func is not None:
                                    print_func(f"{timestamp} - {log_message}")

            except (websockets.ConnectionClosed, Exception) as e:
                # Log an error message if an exception occurs while streaming logs
                self._logger.info(f"Connection to logs server closed: {e}")
                self._logger.warning(
                    "Error occurred while streaming logs - will retry connection.",
                    exc_info=True,
                )

                # Brief pause before attempting to reconnect
                await asyncio.sleep(1)


class CplnKubernetesConverter:
    """Convert a Kubernetes job manifest to a Control Plane cron workload manifest."""

    def __init__(
        self,
        logger: logging,
        client: CplnClient,
        org: str,
        namespace: str,
        k8s_job: KubernetesObjectManifest,
    ):
        # Set received parameters
        self._logger = logger
        self._client = client
        self._org = org
        self._namespace = namespace
        self._k8s_job = k8s_job

        # Converter related
        self._is_identity_overridden = False

        # Extract job name from the manifest
        self._k8s_job_name = k8s_job["metadata"]["generateName"]

        # Define identity and policy name
        self._policy_name = f"{self._k8s_job_name}-reveal-policy"
        self._identity_name = f"{self._k8s_job_name}-identity"

        # Define identity and policy links
        self._policy_parent_link = f"/org/{self._org}/policy"
        self._policy_link = f"{self._policy_parent_link}/{self._policy_name}"
        self._identity_parent_link = f"/org/{self._org}/gvc/{self._namespace}/identity"
        self._identity_link = f"{self._identity_parent_link}/{self._identity_name}"

        # Get identity and policy manifests
        self._policy = self._get_policy()
        self._identity = self._get_identity()

        # Define a list for storing secrets that we will add to the policy later on
        self._secret_names = []

    ### Public Methods ###

    def convert(self):
        """
        Converts the Kubernetes Job Manifest to a Control Plane Cron Workload Manifest.

        Returns:
            Manifest: The Control Plane Cron Workload Manifest.
        """

        # Get the pod spec from the job
        pod_spec = self._k8s_job["spec"]["template"]["spec"]

        # Get and map volumes
        volume_name_to_volume = self._map_and_get_kubernetes_job_volumes(pod_spec)

        # Create workload spec
        workload_spec = {
            "type": "cron",
            "containers": self._build_cpln_workload_containers(
                volume_name_to_volume, pod_spec["containers"][0]
            ),
            "job": self._build_cpln_workload_job_spec(pod_spec),
            "defaultOptions": {
                "capacityAI": True,
                "debug": False,
                "suspend": True,  # Prefect will be the one triggering scheduled jobs, not us
            },
            "firewallConfig": {
                "external": {
                    "inboundAllowCIDR": [],
                    "inboundBlockedCIDR": [],
                    "outboundAllowCIDR": ["0.0.0.0/0"],
                    "outboundAllowHostname": [],
                    "outboundAllowPort": [],
                    "outboundBlockedCIDR": [],
                },
                "internal": {
                    "inboundAllowType": "none",
                    "inboundAllowWorkload": [],
                },
            },
        }

        # Set identity link and override default identity
        if pod_spec.get("serviceAccountName"):
            # Indicate that the default identity has been overridden
            self._is_identity_overridden = True

            # Extract the service account name
            service_account_name = pod_spec["serviceAccountName"]

            # Set the identity link
            workload_spec[
                "identityLink"
            ] = f"{self._identity_parent_link}/{service_account_name}"
        else:
            # Populate secret names and attach them to the policy
            for secret_name in self._secret_names:
                # Update the policy manifest with a new secret link
                self._policy["targetLinks"].append(f"//secret/{secret_name}")

            # Set the default identity link
            workload_spec["identityLink"] = self._identity_link

        # Return the Control Plane cron workload manifest
        return {
            "kind": "workload",
            "name": self._k8s_job_name,
            "tags": self._k8s_job["metadata"]["labels"],
            "spec": workload_spec,
        }

    def create_reliant_resources(self):
        """
        Create and apply the required identity and policy resources.

        This method applies the identity and policy by making requests to the client
        and logs the creation of each resource.
        """

        # Skip apply if identity is overridden
        if self._is_identity_overridden:
            return

        # Apply identity individually
        self._client.put(self._identity_parent_link, self._identity)
        self._logger.info(f"Created {self._identity_link}")

        # Apply policy individually
        self._client.put(self._policy_parent_link, self._policy)
        self._logger.info(f"Created {self._policy_link}")

    def delete_reliant_resources(self):
        """
        Delete the previously created identity and policy resources.

        This method removes the identity and policy by making delete requests to the client
        and logs the deletion of each resource.
        """

        # Skip deletion if no identity/policy were created
        if self._is_identity_overridden:
            return

        # Delete identity individually
        self._client.delete(self._identity_link)
        self._logger.info(f"Deleted {self._identity_link}")

        # Delete policy individually
        self._client.delete(self._policy_link)
        self._logger.info(f"Deleted {self._policy_link}")

    ### Private Methods ###

    def _get_identity(self):
        """
        Construct and return the identity manifest.

        Returns:
            dict: A dictionary representing the identity manifest, including its kind and name.
        """

        # Construct and return the identity manifest
        return {"kind": "identity", "name": self._identity_name}

    def _get_policy(self):
        """
        Construct and return the policy manifest.

        Returns:
            dict: A dictionary representing the policy manifest, including its name,
            target kind, target links, and bindings that grant reveal permissions.
        """

        # Construct and return the policy manifest
        return {
            "kind": "policy",
            "name": self._policy_name,
            "targetKind": "secret",
            "targetLinks": [],  # Empty for now, we will fill later when convert() is called
            "bindings": [
                {
                    # Give reveal permissions to the identity that we will later attached to the cron workload
                    "permissions": ["reveal"],
                    "principalLinks": [self._identity_link],
                }
            ],
        }

    def _build_cpln_workload_containers(
        self, kubernetes_volumes: dict, kubernetes_container: dict
    ) -> list:
        """
        Builds a Control Plane cron workload container from a Kubernetes container.

        Args:
            kubernetes_volumes: The Kubernetes Job volumes.
            kubernetes_container: The Kubernetes container.

        Returns:
            list: The Control Plane cron workload containers.
        """

        # Get container resources
        resources = self._convert_kubernetes_job_resources(kubernetes_container)

        # Build workload container
        container = {
            "name": kubernetes_container["name"],
            "image": kubernetes_container["image"],
            "cpu": resources["cpu"],
            "memory": resources["memory"],
            "minCpu": resources["minCpu"],
            "minMemory": resources["minMemory"]
        }

        # Set command if specified
        if kubernetes_container.get("command"):
            container["command"] = " ".join(kubernetes_container["command"])

        # Set args if specified
        if kubernetes_container.get("args"):
            container["args"] = kubernetes_container["args"]

        # Set working directory
        if kubernetes_container.get("workingDir"):
            container["workingDir"] = kubernetes_container["workingDir"]

        # Set environment variables
        if kubernetes_container.get("env"):
            container["env"] = kubernetes_container["env"]

        # Process envFrom attribute and merge it with container env
        if kubernetes_container.get("envFrom"):
            container["env"] += self._process_env_from(kubernetes_container["envFrom"])

        # Set volume mounts
        if kubernetes_container.get("volumeMounts"):
            container["volumes"] = self._build_cpln_workload_volumes(
                kubernetes_volumes, kubernetes_container["volumeMounts"]
            )

        # Set lifecycle
        if kubernetes_container.get("lifecycle"):
            container["lifecycle"] = kubernetes_container["lifecycle"]

        # Return the containers list
        return [container]

    def _process_env_from(self, kubernetes_env_from: List[dict]) -> List[dict]:
        """
        Process environment variables from Kubernetes environment sources.

        Args:
            kubernetes_env_from (List[dict]): A list of environment sources that can reference
            ConfigMaps or Secrets.

        Returns:
            List[dict]: A list of processed environment variables extracted from secrets.
        """

        # Construct the result
        env = []

        # Iterate over each envFrom source
        for source in kubernetes_env_from:
            if "configMapRef" not in source and "secretRef" not in source:
                continue

            # Prepare a variable to store the name of the secret
            secret_name = ""

            # Extract secret name if found, skip source otherwise
            if source.get("configMapRef"):
                secret_name = source["configMapRef"].get("name", "")
            elif source.get("secretRef"):
                secret_name = source["secretRef"].get("name", "")
            else:
                # In case there are no secret reference, there is nothing to do basically, let's skip this source
                continue

            # Secret name cannot be an empty string, throw a ValueError exception
            if not secret_name:
                raise ValueError(
                    "A Secret name cannot be empty in a job manifest. Either configMapRef or secretRef references an empty name in envFrom."
                )

            # Prepare secret link
            secret_link = f"/org/{self._org}/secret/{secret_name}/-reveal"

            # Fetch the secret by its name
            secret = self._client.get(secret_link)

            # Extract secret type
            secret_type = secret["type"]

            # Validate that the secret is of type dictionary
            if secret_type != "dictionary":
                raise ValueError(
                    f"The secret with the name '{secret_name}' that is being referenced in 'envFrom' must be of type 'dictionary', type found '{secret_type}'."
                )

            # For every key in the secret data, create an environment variable and assign it its value
            for key in secret.get("data", {}).keys():
                # Prepare the name of the environment variable
                name = key
                value = f"cpln://secret/{secret_name}.{key}"

                # Append a prefix to the environment variable name if specified in the source
                if source.get("prefix"):
                    name = source["prefix"] + key

                # Add the new environment variable to the result
                env.append({"name": name, "value": value})

            # Finally keep track of this secret so we can create a policy for it later
            self._secret_names.append(secret_name)

        # Return the result
        return env

    def _build_cpln_workload_volumes(
        self, kubernetes_volumes, kubernetes_volume_mounts
    ) -> list:
        """
        Process the volume mounts specified for a container.

        Args:
            kubernetes_volumes: The Kubernetes Job volumes.
            kubernetes_volume_mounts: The Kubernetes container volume mounts.

        Returns:
            list: The Control Plane cron workload volumes.
        """

        # Initialize the volumes list
        volumes = []

        # Process each volume mount
        for k8s_volume_mount in kubernetes_volume_mounts:
            # Skip if the volume is not found
            if not kubernetes_volumes.get(k8s_volume_mount["name"]):
                continue

            # Get the volume
            k8s_volume = kubernetes_volumes[k8s_volume_mount["name"]]

            # Process persistent volume claims
            if k8s_volume.get("persistentVolumeClaim"):
                uri = f"cpln://volumeset/{k8s_volume['persistentVolumeClaim']['claimName']}"
                path = k8s_volume_mount["mountPath"]

                # Append volume to the list
                volumes.append({"uri": uri, "path": path})

                # Skip to the next volume
                continue

            # Process config maps
            if k8s_volume.get("configMap"):
                self._mount_volume(
                    volumes,
                    k8s_volume_mount,
                    k8s_volume["configMap"]["name"],
                    k8s_volume["configMap"].get("items"),
                )

                # Skip to the next volume
                continue

            # Process secrets
            if k8s_volume.get("secret"):
                self._mount_volume(
                    volumes,
                    k8s_volume_mount,
                    k8s_volume["secret"]["secretName"],
                    k8s_volume["secret"].get("items"),
                )

                # Skip to the next volume
                continue

            # Process projected volumes
            if k8s_volume.get("projected"):
                # Process sources
                for projected_source in k8s_volume["projected"]["sources"]:
                    # Process config maps
                    if projected_source.get("configMap"):
                        self._mount_volume(
                            volumes,
                            k8s_volume_mount,
                            projected_source["configMap"]["name"],
                            projected_source["configMap"].get("items"),
                        )
                    # Process secrets
                    if projected_source.get("secret"):
                        self._mount_volume(
                            volumes,
                            k8s_volume_mount,
                            projected_source["secret"]["name"],
                            projected_source["secret"].get("items"),
                        )

                # Skip to the next volume
                continue

            # Mount a shared volume
            if k8s_volume.get("emptyDir"):
                uri = f"scratch://{k8s_volume['name']}"
                path = k8s_volume_mount["mountPath"]

                # Append subpath if it exists
                if k8s_volume_mount.get("subPath"):
                    path = f"{path}/{k8s_volume_mount['subPath']}"

                # Append the volume to the list
                volumes.append({"uri": uri, "path": path})

            # Handle next volume

        # Return the volumes list
        return volumes

    def _mount_volume(
        self,
        volumes: List[dict],
        k8s_volume_mount: dict,
        secret_name: str,
        volume_items: Optional[List[dict]] = None,
    ) -> None:
        """
        Mount a volume to the container.

        Args:
            volumes: The list of volumes to be updated.
            k8s_volume_mount: The Kubernetes volume mount specification.
            secret_name: The name of the secret to be mounted.
            volume_items: Optional list of volume items.
        """

        # Prepare volume
        uri = f"cpln://secret/{secret_name}"
        path = k8s_volume_mount["mountPath"]

        # Keep track of this secret so we can create a policy for it later
        self._secret_names.append(secret_name)

        # Process items and return
        if volume_items:
            # Iterate over each item and add a new volume
            for item in volume_items:
                # If a sub path is specified, then we will only pick and handle one item
                if k8s_volume_mount.get("subPath"):
                    # Find the path that the sub path is referencing
                    if k8s_volume_mount["subPath"] == item["path"]:
                        # Add new volume
                        volumes.append(
                            {
                                "uri": f"{uri}.{item['key']}",
                                "path": f"{path}/{item['path']}",
                            }
                        )

                        # Exit iteration because we have found the item we are looking for
                        break

                    # Skip to next iteration in order to find the specified sub path
                    continue

                # Add a new volume for each item
                volumes.append(
                    {"uri": f"{uri}.{item['key']}", "path": f"{path}/{item['path']}"}
                )

            # Volume items case has been handled, return
            return

        # Reference secret property
        if k8s_volume_mount.get("subPath"):
            uri = f"{uri}.{k8s_volume_mount['subPath']}"

        # Add a new volume
        volumes.append({"uri": uri, "path": path})

    ### Private Static Methods ###

    @staticmethod
    def _map_and_get_kubernetes_job_volumes(pod_spec: dict) -> dict:
        """
        Map Kubernetes job spec volumes to their names.

        Args:
            pod_spec: The Kubernetes job spec.

        Returns:
            dict: The volume name to volume mapping.
        """

        # Initialize the volume name to volume mapping
        volume_name_to_volume = {}

        # Map volume names to volumes
        for volume in pod_spec.get("volumes", []):
            volume_name_to_volume[volume["name"]] = volume

        # Return the volume name to volume mapping
        return volume_name_to_volume

    @staticmethod
    def _build_cpln_workload_job_spec(pod_spec: dict) -> dict:
        """
        Build Control Plane cron workload job spec from Kubernetes spec.

        Args:
            pod_spec: The Kubernetes job spec.

        Returns:
            dict: The Control Plane cron workload job spec.
        """

        # Job spec defaults
        schedule = "* * * * *"
        concurrency_policy = "Forbid"
        history_limit = 5
        restart_policy = "Never"

        # Define job spec
        job_spec = {
            "schedule": schedule,
            "concurrencyPolicy": concurrency_policy,
            "historyLimit": history_limit,
            "restartPolicy": restart_policy,
        }

        # Set restart policy
        if pod_spec.get("restartPolicy"):
            job_spec["restartPolicy"] = pod_spec["restartPolicy"]

        # Set active deadline seconds if found
        if pod_spec.get("activeDeadlineSeconds"):
            job_spec["activeDeadlineSeconds"] = pod_spec["activeDeadlineSeconds"]

        # Return job spec
        return job_spec

    @staticmethod
    def _convert_kubernetes_job_resources(container: dict) -> dict:
        """
        Convert Kubernetes job resources to Control Plane resources. Defaults to 50m CPU and 128Mi.

        Args:
            container: The Kubernetes container.

        Returns:
            dict: The Control Plane resources.
        """

        # Pick default values
        cpu = "500m"
        memory = "512Mi"

        # Get the resources from the container
        if container.get("resources", {}).get("limits"):
            # Get the CPU limits
            if container["resources"]["limits"].get("cpu"):
                cpu = container["resources"]["limits"]["cpu"]

            # Get the memory limits
            if container["resources"]["limits"].get("memory"):
                memory = container["resources"]["limits"]["memory"]

        min_cpu, min_memory = cpu, memory

        if container.get("resources", {}).get("requests"):
            # Get the Min CPU limits
            if container["resources"]["requests"].get("cpu"):
                min_cpu = container["resources"]["requests"]["cpu"]

            # Get the Min memory limits
            if container["resources"]["requests"].get("memory"):
                min_memory = container["resources"]["requests"]["memory"]

        # Return the resources
        return {
            "cpu": cpu,
            "memory": memory,
            "minCpu": min_cpu,
            "minMemory": min_memory

        }


class CplnInfrastructureResult(InfrastructureResult):
    """Contains information about the final state of a completed Kubernetes Job"""


class CplnInfrastructure(Infrastructure):
    """
    Runs a command as a Control Plane Job.

    Attributes:
        config: An optional Control Plane config to use for this job.
        command: A list of strings specifying the command to run in the container to
            start the flow run. In most cases you should not override this.
        customizations: A list of JSON 6902 patches to apply to the base Job manifest.
        env: Environment variables to set for the container.
        image: An optional string specifying the image reference of a container image
            to use for the job, for example, docker.io/prefecthq/prefect:2-latest. The
            behavior is as described in https://kubernetes.io/docs/concepts/containers/images/#image-names.
            Defaults to the Prefect image.
        job: The base manifest for the Kubernetes Job.
        job_watch_timeout_seconds: Number of seconds to wait for the job to complete
            before marking it as crashed. Defaults to `None`, which means no timeout will be enforced.
        labels: An optional dictionary of labels to add to the job.
        name: An optional name for the job.
        namespace: An optional string signifying the Control Plane GVC to use.
        pod_watch_timeout_seconds: Number of seconds to watch for pod creation before timing out (default 60).
        service_account_name: An optional string specifying which Control Plane identity to use.
        stream_output: If set, stream output from the job to local standard output.
    """

    _block_type_name = "Control Plane Infrastructure"
    _logo_url = "https://console.cpln.io/resources/logos/controlPlaneLogoOnly.svg"
    _documentation_url = "https://docs.controlplane.com"
    _api_dns_name: Optional[str] = None  # Replaces 'localhost' in API URL

    type: Literal["cpln-infrastructure"] = Field(
        default="cpln-infrastructure", description="The type of infrastructure."
    )
    config: Optional[CplnInfrastructureConfig] = Field(
        default=None, description="The Control Plane config to use for this job."
    )
    org: Optional[str] = Field(
        default_factory=lambda: os.getenv("CPLN_ORG"),
        description=(
            "The Control Plane organization to create jobs within. "
            "Defaults to the value in the environment variable CPLN_ORG. "
            "If you are hosting the worker on Control Plane, the environment variable will be automatically injected to your workload."
        ),
    )
    namespace: Optional[str] = Field(
        default_factory=lambda: os.getenv("CPLN_GVC"),
        description=(
            "The Control Plane GVC to create jobs within. Defaults to the value in the environment variable CPLN_GVC. "
            "If you are hosting the worker on Control Plane, the environment variable will be automatically injected to your workload."
        ),
    )
    location: Optional[str] = Field(
        default_factory=lambda: os.getenv("CPLN_LOCATION", "").split("/")[-1],
        description=(
            "The Control Plane GVC location. Defaults to the value in the environment variable CPLN_LOCATION. "
            "If the location is still not found, the first location of the specified GVC will be used. "
            "If you are hosting the worker on Control Plane, the environment variable will be automatically injected to your workload."
        ),
    )
    service_account_name: Optional[str] = Field(
        default=None, description="The Control Plane identity to use for this job."
    )
    job: KubernetesObjectManifest = Field(
        default_factory=lambda: CplnInfrastructure.base_job_manifest(),
        description="The base manifest for the Kubernetes Job.",
        title="Base Job Manifest",
    )
    customizations: JsonPatch = Field(
        default_factory=lambda: JsonPatch([]),
        description="A list of JSON 6902 patches to apply to the base Job manifest.",
    )
    image: Optional[str] = Field(
        default=None,
        description=(
            "The image reference of a container image to use for the job, for example,"
            " `docker.io/prefecthq/prefect:2-latest`.The behavior is as described in"
            " the Kubernetes documentation and uses the latest version of Prefect by"
            " default, unless an image is already present in a provided job manifest."
        ),
    )
    job_watch_timeout_seconds: Optional[int] = Field(
        default=None,
        description=(
            "Number of seconds to wait for the job to complete before marking it as"
            " crashed. Defaults to `None`, which means no timeout will be enforced."
        ),
    )
    pod_watch_timeout_seconds: int = Field(
        default=60,
        description="Number of seconds to watch for pod creation before timing out.",
    )
    stream_output: bool = Field(
        default=True,
        description=(
            "If set, output will be streamed from the job to local standard output."
        ),
    )

    # Support serialization of the 'JsonPatch' type
    class Config:
        arbitrary_types_allowed = True
        json_encoders = {JsonPatch: lambda p: p.patch}

    def dict(self, *args, **kwargs) -> Dict:
        d = super().dict(*args, **kwargs)
        d["customizations"] = self.customizations.patch
        return d

    ### Validators ###

    @root_validator
    def default_image(cls, values):
        return set_default_image(values)

    @validator("job")
    def ensure_job_includes_all_required_components(
        cls, value: KubernetesObjectManifest
    ):
        return validate_k8s_job_required_components(cls, value)

    @validator("job")
    def ensure_job_has_compatible_values(cls, value: KubernetesObjectManifest):
        return validate_k8s_job_compatible_values(cls, value)

    @validator("customizations", pre=True)
    def cast_customizations_to_a_json_patch(
        cls, value: Union[List[Dict], JsonPatch, str]
    ) -> JsonPatch:
        return cast_k8s_job_customizations(cls, value)

    ### Class Methods ###

    @classmethod
    def base_job_manifest(cls) -> KubernetesObjectManifest:
        """Produces the bare minimum allowed Job manifest"""
        return {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {"labels": {}},
            "spec": {
                "template": {
                    "spec": {
                        "parallelism": 1,
                        "completions": 1,
                        "restartPolicy": "Never",
                        "containers": [
                            {
                                "name": "prefect-job",
                                "env": [],
                            }
                        ],
                    }
                }
            },
        }

    @classmethod
    def job_from_file(cls, filename: str) -> KubernetesObjectManifest:
        """Load a Kubernetes Job manifest from a YAML or JSON file."""
        with open(filename, "r", encoding="utf-8") as f:
            return yaml.load(f, yaml.SafeLoader)

    @classmethod
    def customize_from_file(cls, filename: str) -> JsonPatch:
        """Load an RFC 6902 JSON patch from a YAML or JSON file."""
        with open(filename, "r", encoding="utf-8") as f:
            return JsonPatch(yaml.load(f, yaml.SafeLoader))

    ### Public Methods ###

    @sync_compatible
    async def run(
        self,
        task_status: Optional[anyio.abc.TaskStatus] = None,
    ) -> CplnInfrastructureResult:
        # Raise a ValueError if no command was specified
        if not self.command:
            raise ValueError("Control Plane job cannot be run with an empty command.")

        # Raise a ValueError if org is not specified
        if not self.org:
            raise ValueError(
                "Control Plane job cannot be run with an empty org. Please specify an organization in the infrastructure block."
            )

        # Raise a ValueError if namespace is not specified
        if not self.namespace:
            raise ValueError(
                "Control Plane job cannot be run with an empty namespace. Please specify a namespace in the infrastructure block."
            )

        # Raise a ValueError if location is not specified
        if not self.location:
            raise ValueError(
                "Control Plane job cannot be run with an empty location. Please specify a location in the infrastructure block."
            )

        # Build the Kubernetes job manifest
        k8s_job = self._build_job()

        # Get the client from the configuration
        client = self._get_cpln_client()

        # Log a message to indicate that the job is being created
        self.logger.info("Creating Control Plane job...")

        # Initialize the Control Plane Kubernetes converter
        self.cpln_k8s_converter = CplnKubernetesConverter(
            self.logger, client, self.org, self.namespace, k8s_job
        )

        # Create the cron workload
        workload = self._create_workload(self.cpln_k8s_converter, client)

        # Extract the job name from the job manifest
        workload_name = workload["name"]

        # Log a message to indicate that we are waiting for workload readiness
        self.logger.info(
            f"Waiting for the cron workload '{workload_name}' to become ready..."
        )

        # Wait for the workload to become ready before starting the job
        self._wait_until_ready(workload_name, client)

        # Log a message to indicate that workload is ready
        self.logger.info("Workload is ready!")

        # Start the job
        job_id = self._start_job(client, workload)

        # Log the successful start of the job and the job ID
        self.logger.info(
            f"[CplnInfrastructure] Started job with ID: {job_id} in location {self.location}"
        )

        # Get infrastructure pid
        pid = self._get_infrastructure_pid(workload)

        # Indicate that the job has started
        if task_status is not None:
            task_status.started(pid)

        # Monitor the job until completion
        status_code = await self._watch_job(client, workload_name, job_id)

        # Delete the cron workload
        await self._delete_workload(pid)

        # Delete the reliant resources as well
        self.cpln_k8s_converter.delete_reliant_resources()

        # Return infrastructure result with pid and status code
        return CplnInfrastructureResult(identifier=pid, status_code=status_code)

    async def kill(self, infrastructure_pid: str, grace_seconds: int = 30):
        """
        Deletes the cron workload for a cancelled flow run based on the provided infrastructure PID.

        Args:
            infrastructure_pid: The PID of the infrastructure.
            grace_seconds: The number of seconds to wait before killing the job.
        """

        # Delete the workload
        await self._delete_workload(infrastructure_pid, grace_seconds)

        # Delete the reliant resources if the converter is found
        if self.cpln_k8s_converter:
            self.cpln_k8s_converter.delete_reliant_resources()

    def preview(self):
        return yaml.dump(self._build_job())

    def get_corresponding_worker_type(self):
        return "cpln"

    ### Private Methods ###

    def _get_cpln_client(self) -> CplnClient:
        """
        Get an authenticated Control Plane API client.

        Args:
            configuration: The configuration to retrieve the API client from.

        Returns:
            CplnClient: A configured Control Plane API client.
        """

        # If the user has configured a CplnConfig, then get the API client
        if self.config is not None:
            return self.config.get_api_client()

        # Create a new API client using the CPLN_TOKEN environment variable for authentication
        return CplnInfrastructureConfig().get_api_client()

    def _build_job(self) -> KubernetesObjectManifest:
        """Builds the Kubernetes Job Manifest"""
        job_manifest = copy.copy(self.job)
        job_manifest = self._shortcut_customizations().apply(job_manifest)
        job_manifest = self.customizations.apply(job_manifest)
        return job_manifest

    @retry(
        stop=stop_after_attempt(RETRY_MAX_ATTEMPTS),
        wait=wait_fixed(RETRY_MIN_DELAY_SECONDS)
        + wait_random(
            RETRY_MIN_DELAY_JITTER_SECONDS,
            RETRY_MAX_DELAY_JITTER_SECONDS,
        ),
        reraise=True,
    )
    def _create_workload(
        self, cpln_k8s_converter: CplnKubernetesConverter, client: CplnClient
    ) -> CplnObjectManifest:
        """
        Creates a Control Plane job from a Kubernetes job manifest.

        Args:
            configuration (CplnWorkerJobConfiguration): The configuration containing the
                job manifest and associated parameters.
            client (CplnClient): The API client used to communicate with the Control Plane.

        Returns:
            constants.Manifest: The manifest of the created or updated job.

        Raises:
            InfrastructureError: If there is an issue retrieving or creating the job.
            requests.exceptions.HTTPError: If there are HTTP errors that cannot be handled.
        """

        # Convert the Kubernetes manifest to a Control Plane workload manifest
        workload_manifest = cpln_k8s_converter.convert()

        # Create the reliant resources before creating the workload
        cpln_k8s_converter.create_reliant_resources()

        # Extract the name of the workload from the manifest
        name = workload_manifest["name"]

        try:
            # Attempt to retrieve the workload by its name if it already exists
            client.get(
                f"/org/{self.org}/gvc/{self.namespace}/workload/{name}",
                True,
            )

            # If no exception has been thrown, then update the workload with the new manifest
            client.put(
                f"/org/{self.org}/gvc/{self.namespace}/workload/",
                workload_manifest,
            )

            # Return the updated workload
            return client.get(f"/org/{self.org}/gvc/{self.namespace}/workload/{name}")

        except requests.exceptions.HTTPError as e:
            # If the workload doesn't exist, raise an error if the status code is not 404
            if e.response.status_code != 404:
                raise InfrastructureError(
                    f"Failed to retrieve workload '{name}': {str(e.response.text)}"
                ) from e

        # If workload doesn't exist, create a new one
        client.post(
            f"/org/{self.org}/gvc/{self.namespace}/workload",
            workload_manifest,
            timeout=self.pod_watch_timeout_seconds,
        )

        # Retrieve and return the newly created workload for confirmation
        return client.get(f"/org/{self.org}/gvc/{self.namespace}/workload/{name}")

    def _start_job(
        self,
        client: CplnClient,
        manifest: CplnObjectManifest,
    ):
        """
        Starts a new job in the specified location and workload.

        Args:
            configuration (CplnWorkerJobConfiguration): The configuration containing the
                job manifest and associated parameters.
            client (CplnClient): The API client used to communicate with the Control Plane.
            manifest: The Control Plane cron workload manifest.

        Returns:
            The job ID of the newly started job.
        """

        # Extract the name of the workload from the manifest
        name = manifest["name"]

        # Construct the path to start a new job in the specified location and workload
        path = f"/org/{self.org}/gvc/{self.namespace}/workload/{name}/-command"

        # Construct the command to start a new job
        command = {
            "type": "runCronWorkload",
            "spec": {"location": self.location, "containerOverrides": []},
        }

        # Make a POST request to start a new job in the specified location and workload
        response = client.post(path, command)

        # If the response status code is not 201, raise an exception with the response text
        if response.status_code != 201:
            raise Exception(f"Failed to start job: {response.text}")

        # Extract the job ID from the response headers
        id = response.headers["location"].split("/")[-1]

        # Set the job ID and exit the function
        return id

    def _wait_until_ready(self, workload_name: str, client: CplnClient):
        """
        Wait until the created workload is ready.

        This method repeatedly fetches data from the deployment URL of the workload,
        checking if it is ready. If not ready, it waits before retrying, up to the
        specified timeout in pod_watch_timeout_seconds.

        Args:
            workload_name (str): The workload name.
            client (CplnClient): The Control Plane API client.

        Raises:
            TimeoutError: If the endpoint does not become ready within the timeout period.
        """

        # Record the start time to track how long the function has been running
        start_time = time.time()

        # Construct the workload link
        workload_link = f"/org/{self.org}/gvc/{self.namespace}/workload/{workload_name}"

        # Construct the workload deployments link
        workload_deployments_link = f"{workload_link}/deployment"

        # Start an infinite loop that will keep checking until the timeout is reached
        while True:
            # Fetch workload deployments
            deployment_list = client.get(workload_deployments_link)

            # Check if the response indicates readiness
            if self._check_workload_readiness(deployment_list):
                return

            # Check if timeout has been exceeded
            elapsed_time = time.time() - start_time
            if elapsed_time >= self.pod_watch_timeout_seconds:
                raise InfrastructureError(
                    f"Timeout exceeded while waiting for {workload_deployments_link} to become ready."
                )

            # Wait before the next attempt
            time.sleep(RETRY_WORKLOAD_READY_CHECK_SECONDS)

    def _check_workload_readiness(self, deployment_list) -> bool:
        """
        Check if the selected workload location is ready.

        This method iterates over a list of deployments and checks whether the
        deployment associated with the selected location is marked as ready.

        Args:
            deployment_list (dict): A dictionary containing deployment details,
                                    where each deployment has a name and readiness status.

        Returns:
            bool: True if the selected location is ready, False otherwise.
        """

        # Iterate over each deployment and see if the current selected location is ready or not
        for deployment in deployment_list["items"]:
            # Skip any other location and look for the selected location
            if deployment["name"] != self.location:
                continue

            # If we got here, then the location was found, return the value of the ready attribute
            return deployment.get("status", {}).get("ready", False)

        # If deployment was not found, return False
        return False

    async def _watch_job(
        self,
        client: CplnClient,
        workload_name: str,
        job_id: str,
    ) -> int:
        """
        Watches and monitors a job on the Control Plane platform, logging its status
        and handling its lifecycle stages.

        Args:
            logger (logging.Logger): Logger instance for recording job status updates.
            workload_name (str): The name of the job being monitored.
            job_id (str): The unique ID of the job being monitored.
            configuration (CplnWorkerJobConfiguration): The configuration object containing
                details about the job's namespace and organization.
            client (CplnClient): The API client for interacting with the Control Plane.

        Returns:
            int: A status code representing the final state of the job:
                - 0: Job completed successfully.
                - 1: Job failed.
                - 2: Job was cancelled.
                - 3: Job is still running but logs can no longer be streamed.
        """

        # Log a message to indicate that the job is being monitored
        self.logger.info(f"Job {workload_name!r}: Monitoring job...")

        # Initialize the logs monitor to stream logs for the specified job
        logs_monitor = CplnLogsMonitor(
            self.logger,
            client,
            self.org,
            self.namespace,
            self.location,
            workload_name,
            job_id,
        )

        try:
            # Wait for the job to complete and handle logs
            await asyncio.wait_for(
                logs_monitor.monitor(lambda message: print(message)),
                timeout=self.job_watch_timeout_seconds,
            )
        except asyncio.TimeoutError:
            self.logger.error(
                f"Job {workload_name!r}: Job did not complete within "
                f"timeout of {self.job_watch_timeout_seconds}s."
            )
            return -1

        # Make a GET request to fetch the job status
        data = client.get(
            f"/org/{self.org}/gvc/{self.namespace}/workload/{workload_name}/-command/{job_id}"
        )

        # Extract the lifecycle stage from the response data
        lifecycle_stage = data.get("lifecycleStage")

        # Determine the status code if not successful
        if lifecycle_stage == "failed":
            self.logger.error(f"Job {workload_name!r}: Job has failed.")
            return 1

        if lifecycle_stage == "cancelled":
            self.logger.error(f"Job {workload_name!r}: Job has been cancelled.")
            return 2

        if lifecycle_stage == "pending" or lifecycle_stage == "running":
            # Job is still running
            self.logger.error(
                "An error occurred while waiting for the job to complete - exiting...",
                exc_info=True,
            )

            # Return 3 to indicate that the job is still running
            return 3

        # Log a message to indicate that the job has completed successfully
        self.logger.info(f"Job {workload_name!r}: Job has completed successfully.")

        # Return 0 to indicate that the job has completed successfully
        return 0

    async def _delete_workload(
        self,
        infrastructure_pid: str,
        grace_seconds: int = 30,
    ):
        """
        Removes the given Job from the Control Plane platform.

        Args:
            infrastructure_pid: The PID of the infrastructure.
            configuration: The configuration to use when executing the flow run.
            grace_seconds: The number of seconds to wait before killing the job.

        Raises:
            InfrastructureNotAvailable: If the job is running in a different namespace or organization
                than expected.
            InfrastructureNotFound: If the job cannot be found on the Control Plane platform.
            requests.RequestException: If an error occurs during the deletion request.
        """

        # Retrieve the API client to interact with the Control Plane platform
        client: CplnClient = self._get_cpln_client()

        # Parse the infrastructure PID to extract the organization ID, namespace, and job name
        job_org_name, job_namespace, workload_name = self._parse_infrastructure_pid(
            infrastructure_pid
        )

        # Check if the job is running in the expected namespace
        if job_namespace != self.namespace:
            raise InfrastructureNotAvailable(
                f"Unable to kill job {workload_name!r}: The job is running in namespace "
                f"{job_namespace!r} but this worker expected jobs to be running in "
                f"namespace {self.namespace!r} based on the work pool and "
                "deployment configuration."
            )

        # Check if the job is running in the expected organization
        if job_org_name != self.org:
            raise InfrastructureNotAvailable(
                f"Unable to kill job {workload_name!r}: The job is running on another "
                "Control Plane organization than the one specified by the infrastructure PID."
            )

        # Attempt to delete the job's cron workload.
        try:
            # Notify of workload deletion
            self.logger.info(
                f"[CplnInfrastructure] Deleting workload '{workload_name}' in {grace_seconds} seconds."
            )

            # Wait for the grace period before killing the job
            await asyncio.sleep(grace_seconds)

            # Delete the job
            client.delete(
                f"/org/{self.org}/gvc/{self.namespace}/workload/{workload_name}"
            )
        except requests.RequestException as e:
            # Raise an error if the job was not found
            if e.response.status_code == 404:
                raise InfrastructureNotFound(
                    f"Unable to kill job {workload_name!r}: The job was not found."
                ) from e
            else:
                # Raise the original exception if the error is not due to the job not being found
                raise

    def _get_infrastructure_pid(self, workload: CplnObjectManifest) -> str:
        """
        Generates a Control Plane infrastructure PID.
        The PID is in the format: "<org uid>:<namespace>:<job name>".

        Args:
            org (str): The name of the organization.
            gvc (str): The name of the GVC that the job belongs to.
            workload (constants.Manifest): The Control Plane workload manifest.
            client (CplnClient): The API client used to communicate with the Control Plane.

        Returns:
            str: The PID of the infrastructure.
        """

        # Extract the job name
        workload_name = workload["name"]

        # Construct the infrastructure PID and return it
        return f"{self.org}:{self.namespace}:{workload_name}"

    def _parse_infrastructure_pid(
        self, infrastructure_pid: str
    ) -> Tuple[str, str, str]:
        """
        Parse a Control Plane infrastructure PID into its component parts.

        Args:
            infrastructure_pid (str): The infrastructure PID to parse.

        Returns:
            str: An organization ID, namespace, and job name.
        """

        # Split the infrastructure PID into its component parts
        org_name, namespace, workload_name = infrastructure_pid.split(":", 2)

        # Return the organization ID, namespace, and job name
        return org_name, namespace, workload_name

    def _shortcut_customizations(self) -> JsonPatch:
        """Produces the JSON 6902 patch for the most commonly used customizations, like
        image and namespace, which we offer as top-level parameters (with sensible
        default values)"""
        shortcuts = []

        if self.namespace:
            shortcuts.append(
                {
                    "op": "add",
                    "path": "/metadata/namespace",
                    "value": self.namespace,
                }
            )

        if self.image:
            shortcuts.append(
                {
                    "op": "add",
                    "path": "/spec/template/spec/containers/0/image",
                    "value": self.image,
                }
            )

        shortcuts += [
            {
                "op": "add",
                "path": (
                    f"/metadata/labels/{self._slugify_label_key(key).replace('/', '~1', 1)}"
                ),
                "value": self._slugify_label_value(value),
            }
            for key, value in self.labels.items()
        ]

        shortcuts += [
            {
                "op": "add",
                "path": "/spec/template/spec/containers/0/env/-",
                "value": {"name": key, "value": value},
            }
            for key, value in self._get_environment_variables().items()
        ]

        if self.service_account_name:
            shortcuts.append(
                {
                    "op": "add",
                    "path": "/spec/template/spec/serviceAccountName",
                    "value": self.service_account_name,
                }
            )

        if self.command:
            shortcuts.append(
                {
                    "op": "add",
                    "path": "/spec/template/spec/containers/0/args",
                    "value": self.command,
                }
            )

        if self.name:
            shortcuts.append(
                {
                    "op": "add",
                    "path": "/metadata/generateName",
                    "value": self._slugify_name(self.name),
                }
            )
        else:
            # Generate name is required
            shortcuts.append(
                {
                    "op": "add",
                    "path": "/metadata/generateName",
                    "value": (
                        "prefect-job-"
                        # We generate a name using a hash of the primary job settings
                        + stable_hash(
                            *self.command if self.command else "",
                            *self.env.keys(),
                            *[v for v in self.env.values() if v is not None],
                        )
                        + "-"
                    ),
                }
            )

        return JsonPatch(shortcuts)

    def _slugify_name(self, name: str) -> str:
        """
        Slugify text for use as a name.

        Keeps only alphanumeric characters and dashes, and caps the length
        of the slug at 45 chars.

        The 45 character length allows room for the k8s utility
        "generateName" to generate a unique name from the slug while
        keeping the total length of a name below 63 characters, which is
        the limit for e.g. label names that follow RFC 1123 (hostnames) and
        RFC 1035 (domain names).

        Args:
            name: The name of the job

        Returns:
            the slugified job name
        """
        slug = slugify(
            name,
            max_length=45,  # Leave enough space for generateName
            regex_pattern=r"[^a-zA-Z0-9-]+",
        )

        # TODO: Handle the case that the name is an empty string after being
        # slugified.

        return slug

    def _slugify_label_key(self, key: str) -> str:
        """
        Slugify text for use as a label key.

        Keys are composed of an optional prefix and name, separated by a slash (/).

        Keeps only alphanumeric characters, dashes, underscores, and periods.
        Limits the length of the label prefix to 253 characters.
        Limits the length of the label name to 63 characters.

        See https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#syntax-and-character-set

        Args:
            key: The label key

        Returns:
            The slugified label key
        """
        if "/" in key:
            prefix, name = key.split("/", maxsplit=1)
        else:
            prefix = None
            name = key

        name_slug = (
            slugify(name, max_length=63, regex_pattern=r"[^a-zA-Z0-9-_.]+").strip(
                "_-."  # Must start or end with alphanumeric characters
            )
            or name
        )
        # Fallback to the original if we end up with an empty slug, this will allow
        # Kubernetes to throw the validation error

        if prefix:
            prefix_slug = (
                slugify(
                    prefix,
                    max_length=253,
                    regex_pattern=r"[^a-zA-Z0-9-\.]+",
                ).strip("_-.")  # Must start or end with alphanumeric characters
                or prefix
            )

            return f"{prefix_slug}/{name_slug}"

        return name_slug

    def _slugify_label_value(self, value: str) -> str:
        """
        Slugify text for use as a label value.

        Keeps only alphanumeric characters, dashes, underscores, and periods.
        Limits the total length of label text to below 63 characters.

        See https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#syntax-and-character-set

        Args:
            value: The text for the label

        Returns:
            The slugified value
        """
        slug = (
            slugify(value, max_length=63, regex_pattern=r"[^a-zA-Z0-9-_\.]+").strip(
                "_-."  # Must start or end with alphanumeric characters
            )
            or value
        )
        # Fallback to the original if we end up with an empty slug, this will allow
        # Kubernetes to throw the validation error

        return slug

    def _get_environment_variables(self):
        # If the API URL has been set by the base environment rather than the by the
        # user, update the value to ensure connectivity when using a bridge network by
        # updating local connections to use the internal host
        env = {**self._base_environment(), **self.env}

        if (
            "PREFECT_API_URL" in env
            and "PREFECT_API_URL" not in self.env
            and self._api_dns_name
        ):
            env["PREFECT_API_URL"] = (
                env["PREFECT_API_URL"]
                .replace("localhost", self._api_dns_name)
                .replace("127.0.0.1", self._api_dns_name)
            )

        # Drop null values allowing users to "unset" variables
        return {key: value for key, value in env.items() if value is not None}


### Helper Functions ###


async def watch_job_until_completion(
    client: CplnClient,
    org: str,
    gvc: str,
    workload_name: str,
    command_id: str,
    interval_seconds: Optional[int] = None,
) -> str:
    """
    Recursively fetch the command and check the lifecycle value for completion.

    Args:
        client (CplnClient): The Control Plane API client.
        org (str): The organization name.
        gvc (str): The GVC name where the workload lives.
        workload_name (str): The workload name where the command is running.
        command_id (str): The command ID.

    Returns:
        str: The job status upon completion.
    """

    lifecycle_stage = None
    delay_seconds = (
        interval_seconds if interval_seconds is not None else COMMAND_STATUS_CHECK_DELAY
    )

    # Continue monitoring the job status until completion
    while True:
        # Construct the command link
        command_link = (
            f"/org/{org}/gvc/{gvc}/workload/{workload_name}/-command/{command_id}"
        )

        # Make a GET request to fetch the job status
        command = client.get(command_link)

        # Extract the lifecycle stage from the response data and return it
        lifecycle_stage = command["lifecycleStage"]

        # If the job has completed, failed, or cancelled, set the flag to disconnect
        if lifecycle_stage in LIFECYCLE_FINAL_STAGES:
            # Set the completed flag to True to indicate completion
            break

        # Pause for a delay before checking the job status again
        await asyncio.sleep(delay_seconds)

    # Return the final lifecycle stage of the job
    return lifecycle_stage
