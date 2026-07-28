"""
DEPRECATION WARNING:

This module is deprecated as of March 2024 and will not be available after September 2024.
Agents have been replaced by workers, which offer enhanced functionality and better performance.

For upgrade instructions, see https://docs.prefect.io/latest/guides/upgrade-guide-agents-to-workers/.
"""

import inspect
import os
import time
from typing import AsyncIterator, Dict, List, Optional, Set, TypedDict, Union
from uuid import UUID

import anyio
import anyio.abc
import anyio.to_process
import pendulum
import requests

from prefect._internal.compatibility.deprecated import (
    deprecated_class,
)
from prefect.blocks.core import Block
from prefect.blocks.cpln import CplnClient, CplnInfrastructureConfig
from prefect.client.orchestration import PrefectClient, get_client
from prefect.client.schemas.filters import (
    FlowRunFilter,
    FlowRunFilterId,
    FlowRunFilterState,
    FlowRunFilterStateName,
    FlowRunFilterStateType,
    WorkPoolFilter,
    WorkPoolFilterName,
    WorkQueueFilter,
    WorkQueueFilterName,
)
from prefect.client.schemas.objects import (
    DEFAULT_AGENT_WORK_POOL_NAME,
    BlockDocument,
    FlowRun,
    WorkQueue,
)
from prefect.engine import propose_state
from prefect.exceptions import (
    Abort,
    InfrastructureNotAvailable,
    InfrastructureNotFound,
    ObjectNotFound,
)
from prefect.infrastructure import Infrastructure, InfrastructureResult, Process
from prefect.infrastructure.cpln import (
    CPLN_PREFECT_AGENT_TAG_KEY,
    LIFECYCLE_FINAL_STAGES,
    PREFECT_IDENTIFIER_TAG_KEY,
)
from prefect.logging import get_logger
from prefect.settings import PREFECT_AGENT_PREFETCH_SECONDS
from prefect.states import Crashed, Pending, StateType, exception_to_failed_state


class _TerminatingJob(TypedDict):
    stop_command_id: str
    stop_sent_at: float
    workload_link: str
    gvc: str
    flow_run_id: str
    flow_run_state: str
    replica: str
    location: str


@deprecated_class(
    start_date="Mar 2024",
    help="Use a worker instead. Refer to the upgrade guide for more information: https://docs.prefect.io/latest/guides/upgrade-guide-agents-to-workers/.",
)
class PrefectAgent:
    def __init__(
        self,
        work_queues: List[str] = None,
        work_queue_prefix: Union[str, List[str]] = None,
        work_pool_name: str = None,
        prefetch_seconds: int = None,
        default_infrastructure: Infrastructure = None,
        default_infrastructure_document_id: UUID = None,
        limit: Optional[int] = None,
    ) -> None:
        if default_infrastructure and default_infrastructure_document_id:
            raise ValueError(
                "Provide only one of 'default_infrastructure' and"
                " 'default_infrastructure_document_id'."
            )

        self.work_queues: Set[str] = set(work_queues) if work_queues else set()
        self.work_pool_name = work_pool_name
        self.prefetch_seconds = prefetch_seconds
        self.submitting_flow_run_ids = set()
        self.cancelling_flow_run_ids = set()
        self.scheduled_task_scopes = set()
        self.started = False
        self.logger = get_logger("agent")
        self.task_group: Optional[anyio.abc.TaskGroup] = None
        self.limit: Optional[int] = limit
        self.limiter: Optional[anyio.CapacityLimiter] = None
        self.client: Optional[PrefectClient] = None
        self.cpln_client: Optional[CplnClient] = None
        self.cpln_org: Optional[str] = os.getenv("CPLN_ORG")
        self.cpln_agent_workload: Optional[str] = os.getenv("CPLN_WORKLOAD")
        self.cpln_warned_flow_run_ids: Set[
            UUID
        ] = set()  # Track flow runs we've warned about for invalid PID format
        self._terminating_jobs: Dict[str, _TerminatingJob] = {}
        self._cpln_404_counts: Dict[
            str, int
        ] = {}  # Track consecutive 404s per command_id

        if isinstance(work_queue_prefix, str):
            work_queue_prefix = [work_queue_prefix]
        self.work_queue_prefix = work_queue_prefix

        self._work_queue_cache_expiration: pendulum.DateTime = None
        self._work_queue_cache: List[WorkQueue] = []

        if default_infrastructure:
            self.default_infrastructure_document_id = (
                default_infrastructure._block_document_id
            )
            self.default_infrastructure = default_infrastructure
        elif default_infrastructure_document_id:
            self.default_infrastructure_document_id = default_infrastructure_document_id
            self.default_infrastructure = None
        else:
            self.default_infrastructure = Process()
            self.default_infrastructure_document_id = None

    async def update_matched_agent_work_queues(self):
        if self.work_queue_prefix:
            if self.work_pool_name:
                matched_queues = await self.client.read_work_queues(
                    work_pool_name=self.work_pool_name,
                    work_queue_filter=WorkQueueFilter(
                        name=WorkQueueFilterName(startswith_=self.work_queue_prefix)
                    ),
                )
            else:
                matched_queues = await self.client.match_work_queues(
                    self.work_queue_prefix, work_pool_name=DEFAULT_AGENT_WORK_POOL_NAME
                )

            matched_queues = set(q.name for q in matched_queues)
            if matched_queues != self.work_queues:
                new_queues = matched_queues - self.work_queues
                removed_queues = self.work_queues - matched_queues
                if new_queues:
                    self.logger.info(
                        f"Matched new work queues: {', '.join(new_queues)}"
                    )
                if removed_queues:
                    self.logger.info(
                        f"Work queues no longer matched: {', '.join(removed_queues)}"
                    )
            self.work_queues = matched_queues

    async def get_work_queues(self) -> AsyncIterator[WorkQueue]:
        """
        Loads the work queue objects corresponding to the agent's target work
        queues. If any of them don't exist, they are created.
        """

        # if the queue cache has not expired, yield queues from the cache
        now = pendulum.now("UTC")
        if (self._work_queue_cache_expiration or now) > now:
            for queue in self._work_queue_cache:
                yield queue
            return

        # otherwise clear the cache, set the expiration for 30 seconds, and
        # reload the work queues
        self._work_queue_cache.clear()
        self._work_queue_cache_expiration = now.add(seconds=30)

        await self.update_matched_agent_work_queues()

        for name in self.work_queues:
            try:
                work_queue = await self.client.read_work_queue_by_name(
                    work_pool_name=self.work_pool_name, name=name
                )
            except (ObjectNotFound, Exception):
                work_queue = None

            # if the work queue wasn't found and the agent is NOT polling
            # for queues using a regex, try to create it
            if work_queue is None and not self.work_queue_prefix:
                try:
                    work_queue = await self.client.create_work_queue(
                        work_pool_name=self.work_pool_name, name=name
                    )
                except Exception:
                    # if creating it raises an exception, it was probably just
                    # created by some other agent; rather than entering a re-read
                    # loop with new error handling, we log the exception and
                    # continue.
                    self.logger.exception(f"Failed to create work queue {name!r}.")
                    continue
                else:
                    log_str = f"Created work queue {name!r}"
                    if self.work_pool_name:
                        log_str = (
                            f"Created work queue {name!r} in work pool"
                            f" {self.work_pool_name!r}."
                        )
                    else:
                        log_str = f"Created work queue '{name}'."
                    self.logger.info(log_str)

            if work_queue is None:
                self.logger.error(
                    f"Work queue '{name!r}' with prefix {self.work_queue_prefix} wasn't"
                    " found"
                )
            else:
                self._work_queue_cache.append(work_queue)
                yield work_queue

    async def get_and_submit_flow_runs(self) -> List[FlowRun]:
        """
        The principle method on agents. Queries for scheduled flow runs and submits
        them for execution in parallel.
        """
        if not self.started:
            raise RuntimeError(
                "Agent is not started. Use `async with PrefectAgent()...`"
            )

        self.logger.debug("Checking for scheduled flow runs...")

        before = pendulum.now("utc").add(
            seconds=self.prefetch_seconds or PREFECT_AGENT_PREFETCH_SECONDS.value()
        )

        submittable_runs: List[FlowRun] = []

        if self.work_pool_name:
            responses = await self.client.get_scheduled_flow_runs_for_work_pool(
                work_pool_name=self.work_pool_name,
                work_queue_names=[wq.name async for wq in self.get_work_queues()],
                scheduled_before=before,
            )
            submittable_runs.extend([response.flow_run for response in responses])

        else:
            # load runs from each work queue
            async for work_queue in self.get_work_queues():
                # print a nice message if the work queue is paused
                if work_queue.is_paused:
                    self.logger.info(
                        f"Work queue {work_queue.name!r} ({work_queue.id}) is paused."
                    )

                else:
                    try:
                        queue_runs = await self.client.get_runs_in_work_queue(
                            id=work_queue.id, limit=10, scheduled_before=before
                        )
                        submittable_runs.extend(queue_runs)
                    except ObjectNotFound:
                        self.logger.error(
                            f"Work queue {work_queue.name!r} ({work_queue.id}) not"
                            " found."
                        )
                    except Exception as exc:
                        self.logger.exception(exc)

            submittable_runs.sort(key=lambda run: run.next_scheduled_start_time)

        for flow_run in submittable_runs:
            # don't resubmit a run
            if flow_run.id in self.submitting_flow_run_ids:
                continue

            try:
                if self.limiter:
                    self.limiter.acquire_on_behalf_of_nowait(flow_run.id)
            except anyio.WouldBlock:
                self.logger.info(
                    f"Flow run limit reached; {self.limiter.borrowed_tokens} flow runs"
                    " in progress."
                )
                break
            else:
                self.logger.info(f"Submitting flow run '{flow_run.id}'")
                self.submitting_flow_run_ids.add(flow_run.id)
                self.task_group.start_soon(
                    self.submit_run,
                    flow_run,
                )

        return list(
            filter(lambda run: run.id in self.submitting_flow_run_ids, submittable_runs)
        )

    async def check_for_cancelled_flow_runs(self):
        if not self.started:
            raise RuntimeError(
                "Agent is not started. Use `async with PrefectAgent()...`"
            )

        self.logger.debug("Checking for cancelled flow runs...")

        work_queue_filter = (
            WorkQueueFilter(name=WorkQueueFilterName(any_=list(self.work_queues)))
            if self.work_queues
            else None
        )

        work_pool_filter = (
            WorkPoolFilter(name=WorkPoolFilterName(any_=[self.work_pool_name]))
            if self.work_pool_name
            else WorkPoolFilter(name=WorkPoolFilterName(any_=["default-agent-pool"]))
        )
        named_cancelling_flow_runs = await self.client.read_flow_runs(
            flow_run_filter=FlowRunFilter(
                state=FlowRunFilterState(
                    type=FlowRunFilterStateType(any_=[StateType.CANCELLED]),
                    name=FlowRunFilterStateName(any_=["Cancelling"]),
                ),
                # Avoid duplicate cancellation calls
                id=FlowRunFilterId(not_any_=list(self.cancelling_flow_run_ids)),
            ),
            work_pool_filter=work_pool_filter,
            work_queue_filter=work_queue_filter,
        )

        typed_cancelling_flow_runs = await self.client.read_flow_runs(
            flow_run_filter=FlowRunFilter(
                state=FlowRunFilterState(
                    type=FlowRunFilterStateType(any_=[StateType.CANCELLING]),
                ),
                # Avoid duplicate cancellation calls
                id=FlowRunFilterId(not_any_=list(self.cancelling_flow_run_ids)),
            ),
            work_pool_filter=work_pool_filter,
            work_queue_filter=work_queue_filter,
        )

        cancelling_flow_runs = named_cancelling_flow_runs + typed_cancelling_flow_runs

        if cancelling_flow_runs:
            self.logger.info(
                f"Found {len(cancelling_flow_runs)} flow runs awaiting cancellation."
            )

        for flow_run in cancelling_flow_runs:
            self.cancelling_flow_run_ids.add(flow_run.id)
            self.task_group.start_soon(self.cancel_run, flow_run)

        return cancelling_flow_runs

    async def cancel_run(self, flow_run: FlowRun) -> None:
        """
        Cancel a flow run by killing its infrastructure
        """
        if not flow_run.infrastructure_pid:
            self.logger.error(
                f"Flow run '{flow_run.id}' does not have an infrastructure pid"
                " attached. Cancellation cannot be guaranteed."
            )
            await self._mark_flow_run_as_cancelled(
                flow_run,
                state_updates={
                    "message": (
                        "This flow run is missing infrastructure tracking information"
                        " and cancellation cannot be guaranteed."
                    )
                },
            )
            return

        try:
            infrastructure = await self.get_infrastructure(flow_run)
            if infrastructure.is_using_a_runner:
                self.logger.info(
                    f"Skipping cancellation because flow run {str(flow_run.id)!r} is"
                    " using enhanced cancellation. A dedicated runner will handle"
                    " cancellation."
                )
                return
        except Exception:
            self.logger.exception(
                f"Failed to get infrastructure for flow run '{flow_run.id}'. "
                "Flow run cannot be cancelled."
            )
            # Note: We leave this flow run in the cancelling set because it cannot be
            #       cancelled and this will prevent additional attempts.
            return

        if not hasattr(infrastructure, "kill"):
            self.logger.error(
                f"Flow run '{flow_run.id}' infrastructure {infrastructure.type!r} "
                "does not support killing created infrastructure. "
                "Cancellation cannot be guaranteed."
            )
            return

        self.logger.info(
            f"Killing {infrastructure.type} {flow_run.infrastructure_pid} for flow run "
            f"'{flow_run.id}'..."
        )
        try:
            await infrastructure.kill(flow_run.infrastructure_pid)
        except InfrastructureNotFound as exc:
            self.logger.warning(f"{exc} Marking flow run as cancelled.")
            await self._mark_flow_run_as_cancelled(flow_run)
        except InfrastructureNotAvailable as exc:
            self.logger.warning(f"{exc} Flow run cannot be cancelled by this agent.")
        except Exception:
            self.logger.exception(
                "Encountered exception while killing infrastructure for flow run "
                f"'{flow_run.id}'. Flow run may not be cancelled."
            )
            # We will try again on generic exceptions
            self.cancelling_flow_run_ids.remove(flow_run.id)
            return
        else:
            await self._mark_flow_run_as_cancelled(flow_run)
            self.logger.info(f"Cancelled flow run '{flow_run.id}'!")

    async def _mark_flow_run_as_cancelled(
        self, flow_run: FlowRun, state_updates: Optional[dict] = None
    ) -> None:
        state_updates = state_updates or {}
        state_updates.setdefault("name", "Cancelled")
        state_updates.setdefault("type", StateType.CANCELLED)
        state = flow_run.state.copy(update=state_updates)

        await self.client.set_flow_run_state(flow_run.id, state, force=True)

        # Do not remove the flow run from the cancelling set immediately because
        # the API caches responses for the `read_flow_runs` and we do not want to
        # duplicate cancellations.
        await self._schedule_task(
            60 * 10, self.cancelling_flow_run_ids.remove, flow_run.id
        )

    async def get_infrastructure(self, flow_run: FlowRun) -> Infrastructure:
        deployment = await self.client.read_deployment(flow_run.deployment_id)

        flow = await self.client.read_flow(deployment.flow_id)

        # overrides only apply when configuring known infra blocks
        if not deployment.infrastructure_document_id:
            if self.default_infrastructure:
                infra_block = self.default_infrastructure
            else:
                infra_document = await self.client.read_block_document(
                    self.default_infrastructure_document_id
                )
                infra_block = Block._from_block_document(infra_document)

            # Add flow run metadata to the infrastructure
            prepared_infrastructure = infra_block.prepare_for_flow_run(
                flow_run, deployment=deployment, flow=flow
            )
            return prepared_infrastructure

        ## get infra
        infra_document = await self.client.read_block_document(
            deployment.infrastructure_document_id
        )

        # this piece of logic applies any overrides that may have been set on the
        # deployment; overrides are defined as dot.delimited paths on possibly nested
        # attributes of the infrastructure block
        doc_dict = infra_document.dict()
        infra_dict = doc_dict.get("data", {})
        for override, value in (deployment.job_variables or {}).items():
            nested_fields = override.split(".")
            data = infra_dict
            for field in nested_fields[:-1]:
                data = data[field]

            # once we reach the end, set the value
            data[nested_fields[-1]] = value

        # reconstruct the infra block
        doc_dict["data"] = infra_dict
        infra_document = BlockDocument(**doc_dict)
        infrastructure_block = Block._from_block_document(infra_document)

        # TODO: Here the agent may update the infrastructure with agent-level settings

        # Add flow run metadata to the infrastructure
        prepared_infrastructure = infrastructure_block.prepare_for_flow_run(
            flow_run, deployment=deployment, flow=flow
        )

        return prepared_infrastructure

    async def submit_run(self, flow_run: FlowRun) -> None:
        """
        Submit a flow run to the infrastructure
        """
        ready_to_submit = await self._propose_pending_state(flow_run)

        if ready_to_submit:
            try:
                infrastructure = await self.get_infrastructure(flow_run)
            except Exception as exc:
                self.logger.exception(
                    f"Failed to get infrastructure for flow run '{flow_run.id}'."
                )
                await self._propose_failed_state(flow_run, exc)
                if self.limiter:
                    self.limiter.release_on_behalf_of(flow_run.id)
            else:
                # Wait for submission to be completed. Note that the submission function
                # may continue to run in the background after this exits.
                readiness_result = await self.task_group.start(
                    self._submit_run_and_capture_errors, flow_run, infrastructure
                )

                if readiness_result and not isinstance(readiness_result, Exception):
                    try:
                        # Parse the infrastructure PID to extract CPLN metadata
                        # Format: "org:gvc:workload_name:command_id"
                        pid_str = str(readiness_result)
                        pid_parts = pid_str.split(":", 3)

                        # Build CPLN tags to attach to the flow run for UI visibility
                        cpln_tags = []
                        if len(pid_parts) == 4:
                            org_name, gvc_name, workload_name, command_id = pid_parts
                            cpln_tags = [
                                f"cpln:org:{org_name}",
                                f"cpln:gvc:{gvc_name}",
                                f"cpln:workload:{workload_name}",
                                f"cpln:command:{command_id}",
                            ]

                        # Merge with existing flow run tags
                        updated_tags = list(flow_run.tags or []) + cpln_tags

                        await self.client.update_flow_run(
                            flow_run_id=flow_run.id,
                            infrastructure_pid=pid_str,
                            tags=updated_tags,
                        )
                    except Exception:
                        self.logger.exception(
                            "An error occurred while setting the `infrastructure_pid`"
                            f" on flow run {flow_run.id!r}. The flow run will not be"
                            " cancellable."
                        )

                self.logger.info(f"Completed submission of flow run '{flow_run.id}'")

        else:
            # If the run is not ready to submit, release the concurrency slot
            if self.limiter:
                self.limiter.release_on_behalf_of(flow_run.id)

        self.submitting_flow_run_ids.remove(flow_run.id)

    async def _submit_run_and_capture_errors(
        self,
        flow_run: FlowRun,
        infrastructure: Infrastructure,
        task_status: anyio.abc.TaskStatus = None,
    ) -> Union[InfrastructureResult, Exception]:
        # Note: There is not a clear way to determine if task_status.started() has been
        #       called without peeking at the internal `_future`. Ideally we could just
        #       check if the flow run id has been removed from `submitting_flow_run_ids`
        #       but it is not so simple to guarantee that this coroutine yields back
        #       to `submit_run` to execute that line when exceptions are raised during
        #       submission.
        try:
            result = await infrastructure.run(task_status=task_status)
        except Exception as exc:
            if not task_status._future.done():
                # This flow run was being submitted and did not start successfully
                self.logger.exception(
                    f"Failed to submit flow run '{flow_run.id}' to infrastructure."
                )
                # Mark the task as started to prevent agent crash
                task_status.started(exc)
                await self._propose_crashed_state(
                    flow_run, "Flow run could not be submitted to infrastructure"
                )
            else:
                self.logger.exception(
                    f"An error occurred while monitoring flow run '{flow_run.id}'. "
                    "The flow run will not be marked as failed, but an issue may have "
                    "occurred."
                )
            return exc
        finally:
            if self.limiter:
                self.limiter.release_on_behalf_of(flow_run.id)

        if not task_status._future.done():
            self.logger.error(
                f"Infrastructure returned without reporting flow run '{flow_run.id}' "
                "as started or raising an error. This behavior is not expected and "
                "generally indicates improper implementation of infrastructure. The "
                "flow run will not be marked as failed, but an issue may have occurred."
            )
            # Mark the task as started to prevent agent crash
            task_status.started()

        if result.status_code != 0:
            await self._propose_crashed_state(
                flow_run,
                (
                    "Flow run infrastructure exited with non-zero status code"
                    f" {result.status_code}."
                ),
            )

        return result

    async def _propose_pending_state(self, flow_run: FlowRun) -> bool:
        state = flow_run.state
        try:
            state = await propose_state(self.client, Pending(), flow_run_id=flow_run.id)
        except Abort as exc:
            self.logger.info(
                (
                    f"Aborted submission of flow run '{flow_run.id}'. "
                    f"Server sent an abort signal: {exc}"
                ),
            )
            return False
        except Exception:
            self.logger.error(
                f"Failed to update state of flow run '{flow_run.id}'",
                exc_info=True,
            )
            return False

        if not state.is_pending():
            self.logger.info(
                (
                    f"Aborted submission of flow run '{flow_run.id}': "
                    f"Server returned a non-pending state {state.type.value!r}"
                ),
            )
            return False

        return True

    async def _propose_failed_state(self, flow_run: FlowRun, exc: Exception) -> None:
        try:
            await propose_state(
                self.client,
                await exception_to_failed_state(message="Submission failed.", exc=exc),
                flow_run_id=flow_run.id,
            )
        except Abort:
            # We've already failed, no need to note the abort but we don't want it to
            # raise in the agent process
            pass
        except Exception:
            self.logger.error(
                f"Failed to update state of flow run '{flow_run.id}'",
                exc_info=True,
            )

    async def _propose_crashed_state(self, flow_run: FlowRun, message: str) -> None:
        try:
            state = await propose_state(
                self.client,
                Crashed(message=message),
                flow_run_id=flow_run.id,
            )
        except Abort:
            # Flow run already marked as failed
            pass
        except Exception:
            self.logger.exception(f"Failed to update state of flow run '{flow_run.id}'")
        else:
            if state.is_crashed():
                self.logger.info(
                    f"Reported flow run '{flow_run.id}' as crashed: {message}"
                )

    async def _schedule_task(self, __in_seconds: int, fn, *args, **kwargs):
        """
        Schedule a background task to start after some time.

        These tasks will be run immediately when the agent exits instead of waiting.

        The function may be async or sync. Async functions will be awaited.
        """

        async def wrapper(task_status):
            # If we are shutting down, do not sleep; otherwise sleep until the scheduled
            # time or shutdown
            if self.started:
                with anyio.CancelScope() as scope:
                    self.scheduled_task_scopes.add(scope)
                    task_status.started()
                    await anyio.sleep(__in_seconds)

                self.scheduled_task_scopes.remove(scope)
            else:
                task_status.started()

            result = fn(*args, **kwargs)
            if inspect.iscoroutine(result):
                await result

        await self.task_group.start(wrapper)

    # Control Plane Corp. Related ---------------------------------------------------------------

    async def sync_cpln_to_prefect(self):
        """
        Synchronize failed Control Plane (CPLN) jobs with Prefect flow runs.

        This method checks for jobs running on the Control Plane platform that were
        originally created by Prefect. If any of these jobs have failed, completed,
        or no longer exist, but the corresponding Prefect flow runs are still marked
        as running, the flow runs are updated appropriately in Prefect.

        This helps ensure that the Prefect server reflects the true job status
        from the Control Plane platform, preventing discrepancies where failed
        or non-existent jobs appear to be still running in Prefect.
        """

        if not self.started:
            raise RuntimeError(
                "Agent is not started. Use `async with PrefectAgent()...`"
            )

        if not self._ensure_cpln_client():
            self.logger.warning(
                "[CPLN] Sync CPLN→Prefect > Skipping — CPLN client was not created successfully."
            )
            return

        self.logger.debug(
            "[CPLN] Sync CPLN→Prefect > Starting regular job failure check — "
            "failed/missing jobs still marked as running in Prefect will be set to 'crashed'."
        )

        work_queue_filter = (
            WorkQueueFilter(name=WorkQueueFilterName(any_=list(self.work_queues)))
            if self.work_queues
            else None
        )

        work_pool_filter = (
            WorkPoolFilter(name=WorkPoolFilterName(any_=[self.work_pool_name]))
            if self.work_pool_name
            else WorkPoolFilter(name=WorkPoolFilterName(any_=["default-agent-pool"]))
        )

        # Query flow runs in RUNNING state with pagination to ensure none are missed.
        # The Prefect API enforces a max limit of 200 per request.
        running_flow_runs = []
        offset = 0
        page_size = 200
        while True:
            page = await self.client.read_flow_runs(
                flow_run_filter=FlowRunFilter(
                    state=FlowRunFilterState(
                        type=FlowRunFilterStateType(any_=[StateType.RUNNING]),
                    ),
                ),
                work_pool_filter=work_pool_filter,
                work_queue_filter=work_queue_filter,
                limit=page_size,
                offset=offset,
            )
            running_flow_runs.extend(page)
            if len(page) < page_size:
                break
            offset += page_size

        # If the running flow runs list is empty, exit early
        if not running_flow_runs:
            self.logger.debug(
                "[CPLN] Sync CPLN→Prefect > Regular job failure check complete — no running flow runs."
            )
            return

        # Filter for flow runs with infrastructure PIDs (means they were submitted to CPLN)
        running_flow_runs_with_pid = [
            fr for fr in running_flow_runs if fr.infrastructure_pid
        ]

        if not running_flow_runs_with_pid:
            self.logger.debug(
                "[CPLN] Sync CPLN→Prefect > Regular job failure check complete — no flow runs with infrastructure PIDs."
            )
            return

        # Iterate over each flow run and check its CPLN job status directly using the infrastructure PID
        for flow_run in running_flow_runs_with_pid:
            try:
                # Parse the infrastructure PID to extract workload and command info
                # Format: "org:namespace:workload_name:command_id"
                pid_parts = flow_run.infrastructure_pid.split(":", 3)
                if len(pid_parts) != 4:
                    # Only warn once per flow run to avoid log spam
                    if flow_run.id not in self.cpln_warned_flow_run_ids:
                        self.logger.warning(
                            f"[CPLN] Sync CPLN→Prefect > Invalid infrastructure PID format for flow run '{flow_run.id}': {flow_run.infrastructure_pid}"
                        )
                        self.cpln_warned_flow_run_ids.add(flow_run.id)
                    continue

                org_name, gvc_name, workload_name, command_id = pid_parts

                # Only process if org matches
                if org_name != self.cpln_org:
                    continue

                # Construct workload self link
                workload_self_link = (
                    f"/org/{org_name}/gvc/{gvc_name}/workload/{workload_name}"
                )

                # Directly query the command by ID (more reliable than tag query)
                try:
                    command_link = f"{workload_self_link}/-command/{command_id}"

                    # Skip error logging for 404s since they indicate job no longer exists
                    command = self.cpln_client.get(
                        command_link, skipStatusErrorMessage=True
                    )

                    # Get the lifecycle stage of the job
                    lifecycle_stage = command.get("lifecycleStage", "")

                    # If the job has completed, mark the flow run as completed
                    if lifecycle_stage == "completed":
                        self.logger.info(
                            f"[CPLN] Sync CPLN→Prefect > Flow run '{flow_run.id}' job execution command "
                            f"'{command_id}' in workload '{workload_self_link}'. "
                            f"Job lifecycle stage is 'completed' - setting Prefect flow run state to 'completed'."
                        )
                        try:
                            await self._mark_flow_run_as_completed(flow_run)
                        except Exception as e:
                            self.logger.error(
                                f"[CPLN] Sync CPLN→Prefect > Failed to update flow run '{flow_run.id}' state to completed: {e}",
                                exc_info=True,
                            )

                    # If the job has failed, mark the flow run as crashed
                    elif lifecycle_stage == "failed":
                        self.logger.info(
                            f"[CPLN] Sync CPLN→Prefect > Flow run '{flow_run.id}' job execution command "
                            f"'{command_id}' in workload '{workload_self_link}'. "
                            f"Job lifecycle stage is 'failed' - setting Prefect flow run state to 'crashed'."
                        )
                        try:
                            await self._mark_flow_run_as_crashed(flow_run)
                        except Exception as e:
                            self.logger.error(
                                f"[CPLN] Sync CPLN→Prefect > Failed to update flow run '{flow_run.id}' state to crashed: {e}",
                                exc_info=True,
                            )

                    # If the job was cancelled (e.g., directly on CPLN), mark the flow run as cancelled
                    elif lifecycle_stage == "cancelled":
                        self.logger.info(
                            f"[CPLN] Sync CPLN→Prefect > Flow run '{flow_run.id}' job execution command "
                            f"'{command_id}' in workload '{workload_self_link}'. "
                            f"Job lifecycle stage is 'cancelled' - setting Prefect flow run state to 'cancelled'."
                        )
                        try:
                            await self._mark_flow_run_as_cancelled(
                                flow_run,
                                state_updates={
                                    "message": (
                                        f"CPLN job '{command_id}' was cancelled."
                                    )
                                },
                            )
                        except Exception as e:
                            self.logger.error(
                                f"[CPLN] Sync CPLN→Prefect > Failed to update flow run '{flow_run.id}' state to cancelled: {e}",
                                exc_info=True,
                            )

                    # If job is pending, running, or being cancelled — still active, no action needed
                    elif lifecycle_stage in [
                        "pending",
                        "running",
                        "cancellation-requested",
                    ]:
                        pass

                    # Command found successfully — clear any 404 counter for this command
                    self._cpln_404_counts.pop(command_id, None)

                except requests.exceptions.HTTPError as e:
                    if e.response.status_code == 404:
                        # Track consecutive 404s. After 5 cycles, the command is
                        # permanently gone (e.g., workload was cleaned up) — mark
                        # the flow run as crashed so it doesn't stay RUNNING forever.
                        count = self._cpln_404_counts.get(command_id, 0) + 1
                        self._cpln_404_counts[command_id] = count

                        if count >= 5:
                            self.logger.warning(
                                f"[CPLN] Sync CPLN→Prefect > Job '{command_id}' for flow run '{flow_run.id}' "
                                f"returned 404 for {count} consecutive cycles. "
                                f"Marking flow run as crashed — infrastructure no longer exists. "
                                f"(Workload: {workload_self_link})"
                            )
                            try:
                                await self._propose_crashed_state(
                                    flow_run,
                                    f"CPLN job '{command_id}' no longer exists "
                                    f"(404 for {count} consecutive checks).",
                                )
                                self._cpln_404_counts.pop(command_id, None)
                            except Exception as crash_err:
                                self.logger.error(
                                    f"[CPLN] Sync CPLN→Prefect > Failed to mark flow run '{flow_run.id}' as crashed: {crash_err}",
                                    exc_info=True,
                                )
                        else:
                            self.logger.warning(
                                f"[CPLN] Sync CPLN→Prefect > Got 404 when checking job '{command_id}' for flow run '{flow_run.id}' "
                                f"({count}/5). Will retry on the next sync cycle. "
                                f"(Workload: {workload_self_link})"
                            )
                    else:
                        self.logger.error(
                            f"[CPLN] Sync CPLN→Prefect > Failed to check status of job '{command_id}' for flow run '{flow_run.id}': {e.response.text}",
                            exc_info=True,
                        )

            except Exception as e:
                self.logger.error(
                    f"[CPLN] Sync CPLN→Prefect > Error processing running flow run '{flow_run.id}': {e}",
                    exc_info=True,
                )

        self.logger.debug(
            "[CPLN] Sync CPLN→Prefect > Regular job failure check complete."
        )

    async def _mark_flow_run_as_completed(
        self, flow_run: FlowRun, state_updates: Optional[dict] = None
    ) -> None:
        state_updates = state_updates or {}
        state_updates.setdefault("name", "Completed")
        state_updates.setdefault("type", StateType.COMPLETED)
        state = flow_run.state.copy(update=state_updates)

        await self.client.set_flow_run_state(flow_run.id, state, force=True)

    async def _mark_flow_run_as_crashed(
        self, flow_run: FlowRun, state_updates: Optional[dict] = None
    ) -> None:
        state_updates = state_updates or {}
        state_updates.setdefault("name", "Crashed")
        state_updates.setdefault("type", StateType.CRASHED)
        state = flow_run.state.copy(update=state_updates)

        await self.client.set_flow_run_state(flow_run.id, state, force=True)

    async def sync_prefect_to_cpln(self):
        """
        Synchronize Control Plane (CPLN) job executions with Prefect flow run states.

        Uses a CPLN-first approach: queries CPLN for running commands, then checks
        Prefect to see if the corresponding flow run has already completed. If the
        flow run is terminal but the CPLN command is still running (e.g., sidecar
        containers haven't exited), sends stopReplica to terminate the job.

        Agent-scoped via the cpln/prefectAgent tag to avoid race conditions when
        multiple agents run concurrently — each agent only processes commands it created.
        """

        if not self.started:
            raise RuntimeError(
                "Agent is not started. Use `async with PrefectAgent()...`"
            )

        if not self._ensure_cpln_client():
            self.logger.warning(
                "[CPLN] Sync Prefect→CPLN > Skipping — CPLN client was not created successfully."
            )
            return

        if not self.cpln_agent_workload:
            self.logger.warning(
                "[CPLN] Sync Prefect→CPLN > Skipping — CPLN_WORKLOAD env var not set. "
                "Cannot scope sync to this agent's commands."
            )
            return

        n_workloads = 0
        n_commands = 0
        n_stopped = 0
        seen_command_ids = set()

        try:
            # Step 1: Query all Prefect-created workloads
            workload_query = {
                "spec": {
                    "match": "all",
                    "terms": [
                        {
                            "op": "=",
                            "tag": PREFECT_IDENTIFIER_TAG_KEY,
                            "value": "true",
                        }
                    ],
                }
            }

            workload_response = self.cpln_client.post(
                f"/org/{self.cpln_org}/workload/-query", workload_query
            )
            workloads = workload_response.json().get("items", [])

            # Step 2: For each workload, query running commands scoped to this agent
            for workload in workloads:
                try:
                    workload_link = self._get_workload_link(workload)
                    if not workload_link:
                        continue

                    gvc = self._extract_gvc_from_link(workload_link)
                    n_workloads += 1

                    # Query running runCronWorkload commands created by this agent
                    commands = self._query_running_commands(workload_link)

                    for command in commands:
                        try:
                            command_id = command.get("id")
                            if not command_id:
                                continue

                            seen_command_ids.add(command_id)
                            n_commands += 1
                            tags = command.get("tags", {})
                            flow_run_id = tags.get("prefect.io/flow-run-id")

                            # Skip commands without a flow run ID tag
                            if not flow_run_id:
                                self.logger.debug(
                                    f"[CPLN] Sync Prefect→CPLN > Command '{command_id}' in workload "
                                    f"'{workload_link}' has no prefect.io/flow-run-id tag. Skipping."
                                )
                                continue

                            # If already tracked as terminating, check progress
                            if command_id in self._terminating_jobs:
                                self._check_termination_progress(
                                    command_id, workload_link, gvc
                                )
                                continue

                            # Check Prefect for flow run state
                            try:
                                flow_run = await self.client.read_flow_run(
                                    UUID(flow_run_id)
                                )
                            except Exception as e:
                                self.logger.error(
                                    f"[CPLN] Sync Prefect→CPLN > Error checking flow run '{flow_run_id}' "
                                    f"on Prefect: {e} "
                                    f"(Workload: {workload_link}, Command: {command_id})"
                                )
                                continue

                            # Skip if flow run is not in a terminal state
                            if flow_run.state.type not in [
                                StateType.COMPLETED,
                                StateType.FAILED,
                                StateType.CRASHED,
                                StateType.CANCELLED,
                            ]:
                                continue

                            # Out of sync: flow done on Prefect, command still running on CPLN
                            flow_state = flow_run.state.type.value
                            replica = command.get("status", {}).get("replica")
                            location = command.get("spec", {}).get("location")

                            if not replica or not location:
                                self.logger.warning(
                                    f"[CPLN] Sync Prefect→CPLN > Command '{command_id}' missing "
                                    f"status.replica or spec.location. Cannot send stopReplica. "
                                    f"(Workload: {workload_link}, GVC: {gvc}, "
                                    f"Org: {self.cpln_org}, Flow Run: {flow_run_id})"
                                )
                                continue

                            self.logger.warning(
                                f"[CPLN] Sync Prefect→CPLN > Flow run '{flow_run_id}' is '{flow_state}' "
                                f"on Prefect but CPLN command '{command_id}' in workload "
                                f"'{workload_link}' (GVC: {gvc}, Org: {self.cpln_org}) "
                                f"is still 'running'. Sending stopReplica "
                                f"(replica: {replica}, location: {location})."
                            )

                            # Send stopReplica
                            stop_command = {
                                "type": "stopReplica",
                                "spec": {
                                    "replica": replica,
                                    "location": location,
                                },
                            }

                            try:
                                response = self.cpln_client.post(
                                    f"{workload_link}/-command", stop_command
                                )
                                stop_command_id = response.headers.get(
                                    "location", ""
                                ).split("/")[-1]

                                self._terminating_jobs[command_id] = {
                                    "stop_command_id": stop_command_id,
                                    "stop_sent_at": time.time(),
                                    "workload_link": workload_link,
                                    "gvc": gvc,
                                    "flow_run_id": flow_run_id,
                                    "flow_run_state": flow_state,
                                    "replica": replica,
                                    "location": location,
                                }

                                self.logger.info(
                                    f"[CPLN] Sync Prefect→CPLN > Successfully sent stopReplica for command "
                                    f"'{command_id}' (stop command: {stop_command_id}, "
                                    f"replica: {replica}, location: {location}) in workload "
                                    f"'{workload_link}' (GVC: {gvc}, Org: {self.cpln_org}, "
                                    f"Flow Run: {flow_run_id})."
                                )
                                n_stopped += 1

                            except requests.exceptions.HTTPError as e:
                                if (
                                    e.response.status_code == 400
                                    and "already exists" in e.response.text
                                ):
                                    self.logger.info(
                                        f"[CPLN] Sync Prefect→CPLN > stopReplica already in progress for "
                                        f"command '{command_id}' in workload '{workload_link}'. "
                                        f"Skipping. (GVC: {gvc}, Org: {self.cpln_org}, "
                                        f"Flow Run: {flow_run_id})"
                                    )
                                else:
                                    self.logger.error(
                                        f"[CPLN] Sync Prefect→CPLN > Failed to send stopReplica for command "
                                        f"'{command_id}': {e.response.text} "
                                        f"(Workload: {workload_link}, GVC: {gvc}, "
                                        f"Org: {self.cpln_org}, Flow Run: {flow_run_id})"
                                    )

                        except Exception as e:
                            self.logger.error(
                                f"[CPLN] Sync Prefect→CPLN > Error processing command "
                                f"'{command.get('id', 'unknown')}' in workload "
                                f"'{workload_link}': {e}",
                                exc_info=True,
                            )

                except Exception as e:
                    self.logger.error(
                        f"[CPLN] Sync Prefect→CPLN > Error querying commands for workload "
                        f"'{workload.get('name', 'unknown')}': {e}",
                        exc_info=True,
                    )

            # Step 3: Clean up tracking for commands that no longer appear
            stale_tracking = [
                cid for cid in self._terminating_jobs if cid not in seen_command_ids
            ]
            for cid in stale_tracking:
                info = self._terminating_jobs.pop(cid)
                elapsed = int(time.time() - info["stop_sent_at"])
                self.logger.info(
                    f"[CPLN] Sync Prefect→CPLN > Command '{cid}' in workload '{info['workload_link']}' "
                    f"terminated successfully after {elapsed}s. "
                    f"(GVC: {info['gvc']}, Org: {self.cpln_org}, "
                    f"Flow Run: {info['flow_run_id']})"
                )

        except Exception as e:
            self.logger.error(
                f"[CPLN] Sync Prefect→CPLN > Unexpected error during sync cycle: {e}",
                exc_info=True,
            )

        if n_stopped > 0 or len(self._terminating_jobs) > 0:
            self.logger.info(
                f"[CPLN] Sync Prefect→CPLN > Cycle complete. Checked {n_workloads} workloads, "
                f"{n_commands} running commands. Sent {n_stopped} stopReplica commands. "
                f"Tracking {len(self._terminating_jobs)} terminating commands."
            )
        else:
            self.logger.debug(
                f"[CPLN] Sync Prefect→CPLN > Cycle complete. Checked {n_workloads} workloads, "
                f"{n_commands} running commands. No action needed."
            )

    def _query_running_commands(self, workload_link: str) -> List[dict]:
        """
        Query CPLN for running runCronWorkload commands on a workload,
        scoped to this agent via the cpln/prefectAgent tag.
        Handles pagination via nextLink.
        """

        query = {
            "spec": {
                "match": "all",
                "terms": [
                    {"op": "=", "property": "type", "value": "runCronWorkload"},
                    {"op": "=", "property": "lifecycleStage", "value": "running"},
                    {
                        "op": "=",
                        "tag": CPLN_PREFECT_AGENT_TAG_KEY,
                        "value": self.cpln_agent_workload,
                    },
                ],
            }
        }

        max_pages = 100
        result = self.cpln_client.post(f"{workload_link}/-command/-query", query).json()
        commands = list(result.get("items", []))

        # Follow pagination with safety limit
        next_link = self._extract_next_link(result)
        page = 1
        while next_link and page < max_pages:
            result = self.cpln_client.get(next_link)
            commands.extend(result.get("items", []))
            next_link = self._extract_next_link(result)
            page += 1

        if page >= max_pages:
            self.logger.warning(
                f"[CPLN] Sync Prefect→CPLN > Pagination limit ({max_pages}) reached for "
                f"workload '{workload_link}'. Some commands may not have been checked."
            )

        return commands

    def _check_termination_progress(
        self, command_id: str, workload_link: str, gvc: str
    ):
        """Check the status of a previously sent stopReplica command."""

        info = self._terminating_jobs.get(command_id)
        if not info:
            return
        elapsed = int(time.time() - info["stop_sent_at"])
        stop_command_id = info["stop_command_id"]

        try:
            stop_command = self.cpln_client.get(
                f"{workload_link}/-command/{stop_command_id}",
                skipStatusErrorMessage=True,
            )
            stop_lifecycle = stop_command.get("lifecycleStage", "unknown")

            if stop_lifecycle in LIFECYCLE_FINAL_STAGES:
                self._terminating_jobs.pop(command_id)
                self.logger.info(
                    f"[CPLN] Sync Prefect→CPLN > Command '{command_id}' in workload '{workload_link}' "
                    f"terminated successfully after {elapsed}s. "
                    f"(GVC: {gvc}, Org: {self.cpln_org}, "
                    f"Flow Run: {info['flow_run_id']})"
                )
            else:
                self.logger.info(
                    f"[CPLN] Sync Prefect→CPLN > Command '{command_id}' in workload '{workload_link}' "
                    f"is still terminating ({elapsed}s elapsed). Stop command "
                    f"'{stop_command_id}' lifecycle: '{stop_lifecycle}'. "
                    f"(GVC: {gvc}, Org: {self.cpln_org}, "
                    f"Flow Run: {info['flow_run_id']})"
                )

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                # Stop command no longer exists — job was cleaned up
                self._terminating_jobs.pop(command_id)
                self.logger.info(
                    f"[CPLN] Sync Prefect→CPLN > Command '{command_id}' in workload '{workload_link}' "
                    f"terminated (stop command no longer exists) after {elapsed}s. "
                    f"(GVC: {gvc}, Org: {self.cpln_org}, "
                    f"Flow Run: {info['flow_run_id']})"
                )
            else:
                self.logger.warning(
                    f"[CPLN] Sync Prefect→CPLN > Error checking stop command '{stop_command_id}' "
                    f"for command '{command_id}': {e.response.text} "
                    f"(Workload: {workload_link}, GVC: {gvc}, Org: {self.cpln_org})"
                )

    def _get_workload_link(self, workload: dict) -> Optional[str]:
        """Extract the self link from a workload query result."""

        for link in workload.get("links", []):
            if link.get("rel") == "self":
                return link.get("href")
        return None

    def _extract_gvc_from_link(self, workload_link: str) -> str:
        """Extract GVC name from a workload self link like /org/X/gvc/Y/workload/Z."""

        parts = workload_link.split("/")
        try:
            gvc_idx = parts.index("gvc")
            return parts[gvc_idx + 1]
        except (ValueError, IndexError):
            self.logger.debug(
                f"[CPLN] Sync Prefect→CPLN > Could not extract GVC from link: {workload_link}"
            )
            return "unknown"

    @staticmethod
    def _extract_next_link(response: dict) -> Optional[str]:
        """Extract the next pagination link from a CPLN response."""

        for link in response.get("links", []):
            if link.get("rel") == "next":
                return link.get("href")
        return None

    def _get_cpln_client(self) -> Optional[CplnClient]:
        """
        Create and validate a Control Plane (CPLN) client.

        This method attempts to:
        1. Read the CPLN organization name and authentication token from environment variables.
        2. Create a CPLN client using the provided credentials.
        3. Validate access to the platform by fetching the organization details.

        If either the environment variables are missing or validation fails,
        monitoring will be skipped by returning `None`.
        """

        self.logger.info(
            "[CPLN] Init > Creating the CPLN client using the CPLN_TOKEN specified in the environment."
        )

        # Extract the authentication token from the environment
        token = os.getenv("CPLN_TOKEN")

        # If organization name is missing, skip client creation
        if not self.cpln_org:
            self.logger.warning(
                "[CPLN] Init > Failed to create the CPLN client, CPLN_ORG environment variable is not set. "
                "Please set CPLN_ORG so the agent can create the CPLN client."
            )

            # Return None to indicate that the client has not been created successfully
            return None

        # If token is missing, skip client creation
        if not token:
            self.logger.warning(
                "[CPLN] Init > Failed to create the CPLN client, CPLN_TOKEN environment variable is not set. "
                "Please set CPLN_TOKEN so the agent can create the CPLN client."
            )

            # Return None to indicate that the client has not been created successfully
            return None

        # Create the CPLN client
        cpln_client = CplnInfrastructureConfig().get_api_client()

        # Validate platform access by fetching the organization
        try:
            cpln_client.get(f"/org/{self.cpln_org}")
        except requests.exceptions.HTTPError as e:
            self.logger.warning(
                f"[CPLN] Init > Failed to fetch CPLN organization '{self.cpln_org}': {e.response.text}",
                exc_info=True,
            )

            # Return None to indicate that the client has not been created successfully
            return None

        # Return the CPLN client
        return cpln_client

    def _ensure_cpln_client(self) -> bool:
        """
        Return True when a CPLN client is available, creating it if necessary.

        Client creation can fail transiently (e.g. a connection reset while the
        agent boots), so callers retry it on every invocation instead of relying
        on a single attempt at startup.
        """

        # Attempt to create the client if it does not exist yet
        if self.cpln_client is None:
            try:
                self.cpln_client = self._get_cpln_client()
            except Exception as e:
                # Creation must never raise here — sync loops and startup treat
                # an unavailable client as a skip, not an error
                self.logger.warning(
                    f"[CPLN] Init > Failed to create the CPLN client: {e}",
                    exc_info=True,
                )

        # Return whether the client is available
        return self.cpln_client is not None

    # Context management ---------------------------------------------------------------

    async def start(self):
        self.started = True
        self.task_group = anyio.create_task_group()
        self.limiter = (
            anyio.CapacityLimiter(self.limit) if self.limit is not None else None
        )
        self.client = get_client()
        self._ensure_cpln_client()
        await self.client.__aenter__()
        await self.task_group.__aenter__()

    async def shutdown(self, *exc_info):
        self.started = False
        # We must cancel scheduled task scopes before closing the task group
        for scope in self.scheduled_task_scopes:
            scope.cancel()
        await self.task_group.__aexit__(*exc_info)
        await self.client.__aexit__(*exc_info)
        self.task_group = None
        self.client = None
        self.cpln_client = None
        self.cpln_org = None
        self.cpln_agent_workload = None
        self._terminating_jobs.clear()
        self._cpln_404_counts.clear()
        self.cpln_warned_flow_run_ids.clear()
        self.submitting_flow_run_ids.clear()
        self.cancelling_flow_run_ids.clear()
        self.scheduled_task_scopes.clear()
        self._work_queue_cache_expiration = None
        self._work_queue_cache = []

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, *exc_info):
        await self.shutdown(*exc_info)
