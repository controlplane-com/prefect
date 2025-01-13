import type { components } from "@/api/prefect";
import { getQueryService } from "@/api/service";
import { queryOptions } from "@tanstack/react-query";

export type Deployment = components["schemas"]["DeploymentResponse"];
export type DeploymentWithFlow = Deployment & {
	flow?: components["schemas"]["Flow"];
};
export type DeploymentsFilter =
	components["schemas"]["Body_read_deployments_deployments_filter_post"];
export type DeploymentsPaginationFilter =
	components["schemas"]["Body_paginate_deployments_deployments_paginate_post"];

/**
 * Query key factory for deployments-related queries
 *
 * @property {function} all - Returns base key for all deployment queries
 * @property {function} lists - Returns key for all list-type deployment queries
 * @property {function} list - Generates key for specific filtered deployment queries
 * @property {function} join-flow - Generates key for specific filtered deployment queries with flow information
 * @property {function} counts - Returns key for all count-type deployment queries
 * @property {function} count - Generates key for specific filtered count queries
 *
 * ```
 * all        =>   ['deployments']
 * lists      =>   ['deployments', 'list']
 * list       =>   ['deployments', 'list', { ...filter }]
 * join-flow  =>   ['deployments', 'list', 'join-flow' { ...filter }]
 * counts     =>   ['deployments', 'counts']
 * count      =>   ['deployments', 'counts', { ...filter }]
 * ```
 */
export const queryKeyFactory = {
	all: () => ["deployments"] as const,
	lists: () => [...queryKeyFactory.all(), "list"] as const,
	list: (filter: DeploymentsFilter | DeploymentsPaginationFilter) =>
		[...queryKeyFactory.lists(), filter] as const,
	"join-flow": (filter: DeploymentsFilter | DeploymentsPaginationFilter) =>
		[...queryKeyFactory.lists(), "join-flow", filter] as const,
	counts: () => [...queryKeyFactory.all(), "counts"] as const,
	count: (filter: DeploymentsFilter) =>
		[...queryKeyFactory.counts(), filter] as const,
};

const requestPaginateDeployments = async (
	filter: DeploymentsPaginationFilter,
) => {
	const res = await getQueryService().POST("/deployments/paginate", {
		body: filter,
	});
	if (!res.data) {
		throw new Error("'data' expected");
	}
	return res.data;
};

// ----------------------------
//  Query Options Factories
// ----------------------------

/**
 * Builds a query configuration for fetching paginated deployments
 *
 * @param filter - Pagination and filter options including:
 *   - page: Page number to fetch (default: 1)
 *   - limit: Number of items per page (default: 10)
 *   - sort: Sort order for results (default: "NAME_ASC")
 *   - deployments: Optional deployment-specific filters
 * @returns Query configuration object for use with TanStack Query
 *
 * @example
 * ```ts
 * const query = buildPaginateDeploymentsQuery({
 *   page: 2,
 *   limit: 25,
 *   sort: "CREATED_DESC"
 * });
 * ```
 */
export const buildPaginateDeploymentsQuery = (
	filter: DeploymentsPaginationFilter = {
		page: 1,
		limit: 10,
		sort: "NAME_ASC",
	},
) =>
	queryOptions({
		queryKey: queryKeyFactory.list(filter),
		queryFn: () => requestPaginateDeployments(filter),
	});

/**
 * Builds a query configuration for counting deployments based on filter criteria
 *
 * @param filter - Filter options for the deployments count query including:
 *   - offset: Number of items to skip (default: 0)
 *   - sort: Sort order for results (default: "NAME_ASC")
 *   - deployments: Optional deployment-specific filters
 * @returns Query configuration object for use with TanStack Query
 *
 * @example
 * ```ts
 * const query = buildCountDeploymentsQuery({
 *   offset: 0,
 *   limit: 10,
 *   sort: "NAME_ASC",
 *   deployments: {
 *     name: { like_: "my-deployment" }
 *   }
 * });
 * ```
 */
export const buildCountDeploymentsQuery = (
	filter: DeploymentsFilter = { offset: 0, sort: "NAME_ASC" },
) =>
	queryOptions({
		queryKey: queryKeyFactory.list(filter),
		queryFn: async () => {
			const res = await getQueryService().POST("/deployments/count", {
				body: filter,
			});
			return res.data ?? 0;
		},
	});

// list deployments and join flow data associated with the deployment
const requestDeploymentsWithFlowDetails = async (
	deployments: Array<Deployment>,
): Promise<DeploymentWithFlow[]> => {
	const deploymentIds = deployments.map((deployment) => deployment.id);
	const flows = await getQueryService().POST("/flows/filter", {
		body: {
			flows: {
				operator: "or_",
				id: { any_: deploymentIds },
			},
			sort: "NAME_DESC",
			offset: 0,
		},
	});
	if (!flows.data) {
		throw new Error("'data' expected");
	}
	// Convert flow list to map
	const flowMap = new Map<string, components["schemas"]["Flow"]>(
		flows.data.map((flow) => [flow.id, flow]),
	);

	// Normalize data per deployment with deployment & flow
	return deployments.map((deployment) => ({
		...deployment,
		flow: flowMap.get(deployment.flow_id),
	}));
};

/**
 * Builds a query configuration for paginating a list of deployments AND joining its flow data
 *
 * @param filter - Pagination and filter options including:
 *   - page: Page number to fetch (default: 1)
 *   - limit: Number of items per page (default: 10)
 *   - sort: Sort order for results (default: "NAME_ASC")
 *   - deployments: Optional deployment-specific filters
 * @returns Query configuration object for use with TanStack Query
 *
 * @example
 * ```ts
 * const query = buildPaginateDeploymentsWithFlowQuery({
 *   page: 2,
 *   limit: 25,
 *   sort: "CREATED_DESC"
 * });
 * ```
 */
export const buildPaginateDeploymentsWithFlowQuery = (
	filter: DeploymentsPaginationFilter = {
		page: 1,
		limit: 100,
		sort: "NAME_ASC",
	},
) =>
	queryOptions({
		queryKey: queryKeyFactory["join-flow"](filter),
		queryFn: async () => {
			const { results, ...rest } = await requestPaginateDeployments(filter);
			const deploymentsWithFlows =
				await requestDeploymentsWithFlowDetails(results);
			return {
				results: deploymentsWithFlows,
				...rest,
			};
		},
	});
