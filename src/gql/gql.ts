/* eslint-disable */
import * as types from './graphql';
import type { TypedDocumentNode as DocumentNode } from '@graphql-typed-document-node/core';

/**
 * Map of all GraphQL operations in the project.
 *
 * This map has several performance disadvantages:
 * 1. It is not tree-shakeable, so it will include all operations in the project.
 * 2. It is not minifiable, so the string of a GraphQL query will be multiple times inside the bundle.
 * 3. It does not support dead code elimination, so it will add unused operations.
 *
 * Therefore it is highly recommended to use the babel or swc plugin for production.
 * Learn more about it here: https://the-guild.dev/graphql/codegen/plugins/presets/preset-client#reducing-bundle-size
 */
type Documents = {
    "\n  mutation ClusterEnabledSet($id: ObjectID!, $enabled: Boolean!) {\n    clusterEnabledSet(id: $id, enabled: $enabled) {\n      id\n      spec {\n        enabled\n      }\n    }\n  }\n": typeof types.ClusterEnabledSetDocument,
    "\n  mutation ClusterSyncEnabledSet($id: ObjectID!, $syncEnabled: Boolean!) {\n    clusterSyncEnabledSet(id: $id, syncEnabled: $syncEnabled) {\n      id\n      spec {\n        syncEnabled\n      }\n    }\n  }\n": typeof types.ClusterSyncEnabledSetDocument,
    "\n  mutation ClusterCacheClear($id: ObjectID!) {\n    clusterCacheClear(id: $id) {\n      id\n    }\n  }\n": typeof types.ClusterCacheClearDocument,
    "\n  mutation ClusterCachedKindSyncEnabledSet($id: ObjectID!, $syncEnabled: Boolean!) {\n    clusterCachedKindSyncEnabledSet(id: $id, syncEnabled: $syncEnabled) {\n      id\n      spec {\n        syncEnabled\n      }\n    }\n  }\n": typeof types.ClusterCachedKindSyncEnabledSetDocument,
    "\n  mutation ClusterDelete($id: ObjectID!) {\n    clusterDelete(id: $id)\n  }\n": typeof types.ClusterDeleteDocument,
    "\n  mutation ClusterConnectionRetry($id: ObjectID!) {\n    clusterConnectionRetry(id: $id)\n  }\n": typeof types.ClusterConnectionRetryDocument,
    "\n  subscription ClusterConnectionEvents($id: ObjectID!) {\n    eventsWatch(id: $id, category: \"connection\") {\n      type\n      event {\n        id\n        type\n        reason\n        message\n        count\n        firstAt\n        lastAt\n      }\n    }\n  }\n": typeof types.ClusterConnectionEventsDocument,
    "\n  subscription ClusterSyncEvents($id: ObjectID!) {\n    eventsWatch(id: $id, category: \"sync\") {\n      type\n      event {\n        id\n        type\n        reason\n        message\n        count\n        firstAt\n        lastAt\n      }\n    }\n  }\n": typeof types.ClusterSyncEventsDocument,
    "\n  subscription ClusterDiscoveryEvents($id: ObjectID!) {\n    eventsWatch(id: $id, category: \"discovery\") {\n      type\n      event {\n        id\n        type\n        reason\n        message\n        count\n        firstAt\n        lastAt\n      }\n    }\n  }\n": typeof types.ClusterDiscoveryEventsDocument,
    "\n  subscription ClusterCacheSyncStatus($id: ObjectID!, $cacheID: ObjectID!) {\n    clusterCacheSyncStatusWatch(id: $id, cacheID: $cacheID) {\n      discovery {\n        reason\n        message\n      }\n      kinds {\n        apiVersion\n        resource\n        reason\n        message\n        objectCount\n      }\n    }\n  }\n": typeof types.ClusterCacheSyncStatusDocument,
    "\n  subscription ClusterCacheStats($id: ObjectID!, $cacheID: ObjectID!) {\n    clusterCacheStatsWatch(id: $id, cacheID: $cacheID) {\n      exists\n      bytes\n      dbBytes\n      walBytes\n      shmBytes\n      objectCount\n      kindCount\n    }\n  }\n": typeof types.ClusterCacheStatsDocument,
    "\n  subscription ClusterCachedKinds($cacheID: ObjectID!) {\n    clusterCachedKindsWatch(cacheID: $cacheID) {\n      type\n      kind {\n        id\n        spec {\n          apiVersion\n          resource\n        }\n      }\n    }\n  }\n": typeof types.ClusterCachedKindsDocument,
    "\n  subscription ClusterSchedule($id: ObjectID!) {\n    clusterScheduleWatch(id: $id) {\n      nextRequeueAt\n      probing\n    }\n  }\n": typeof types.ClusterScheduleDocument,
    "\n  subscription AuthStateWatch {\n    authStateWatch {\n      authenticated\n      identity {\n        sub\n        email\n        name\n      }\n    }\n  }\n": typeof types.AuthStateWatchDocument,
    "\n  mutation AuthLoginStart {\n    authLoginStart\n  }\n": typeof types.AuthLoginStartDocument,
    "\n  mutation AuthLogout {\n    authLogout\n  }\n": typeof types.AuthLogoutDocument,
    "\n  subscription ClusterCachedDataEventsWatch($id: ObjectID!, $cacheID: ObjectID!) {\n    clusterCachedDataEventsWatch(id: $id, cacheID: $cacheID) {\n      type\n      cacheID\n      event {\n        uid\n        type\n        reason\n        message\n        count\n        firstSeen\n        lastSeen\n        involvedKind\n        involvedNamespace\n        involvedName\n      }\n    }\n  }\n": typeof types.ClusterCachedDataEventsWatchDocument,
    "\n  subscription ClusterCachedDataKindsWatch($id: ObjectID!, $cacheID: ObjectID!) {\n    clusterCachedDataKindsWatch(id: $id, cacheID: $cacheID) {\n      type\n      cacheID\n      kind {\n        apiVersion\n        kind\n        resource\n        scope\n        isCRD\n        count\n        printerColumns {\n          name\n          type\n          jsonPath\n          priority\n        }\n      }\n    }\n  }\n": typeof types.ClusterCachedDataKindsWatchDocument,
    "\n  subscription ClusterCachedDataObjectsWatch(\n    $id: ObjectID!\n    $cacheID: ObjectID!\n    $apiVersion: String!\n    $resource: String!\n  ) {\n    clusterCachedDataObjectsWatch(id: $id, cacheID: $cacheID, apiVersion: $apiVersion, resource: $resource) {\n      type\n      cacheID\n      apiVersion\n      resource\n      object {\n        uid\n        apiVersion\n        kind\n        namespace\n        name\n        creationTimestamp\n        rawJSON\n      }\n    }\n  }\n": typeof types.ClusterCachedDataObjectsWatchDocument,
    "\n  subscription ClustersWatch {\n    clustersWatch {\n      type\n      cluster {\n        id\n        deletionRequestedAt\n        spec {\n          name\n          syncEnabled\n          enabled\n          source {\n            kubeconfig {\n              context\n            }\n          }\n        }\n        status {\n          source {\n            kubeconfig {\n              cluster\n              user\n              isPresent\n              isDefault\n            }\n          }\n          server {\n            uid\n          }\n        }\n        conditions {\n          type\n          status\n          reason\n          message\n          liveness\n          unconfirmed\n          transitionedAt\n        }\n      }\n    }\n  }\n": typeof types.ClustersWatchDocument,
    "\n  subscription ClusterCachesWatch {\n    clusterCachesWatch {\n      type\n      cache {\n        id\n        owner {\n          id\n        }\n        spec {\n          serverUid\n        }\n        # On-disk stats ride clusterCacheStatsWatch, subscribed per expanded row.\n      }\n    }\n  }\n": typeof types.ClusterCachesWatchDocument,
    "\n  subscription ClusterCacheHealthWatch {\n    clusterCacheHealthWatch {\n      cacheID\n      status\n      reason\n      unhealthyKindRefs {\n        apiVersion\n        resource\n      }\n      totalKinds\n      unhealthyKinds\n      pausedKinds\n      lastUpdateAt\n      lastLiveAt\n    }\n  }\n": typeof types.ClusterCacheHealthWatchDocument,
    "\n  subscription ChatStream($input: ChatInput!) {\n    chatStream(input: $input) {\n      delta\n      done\n    }\n  }\n": typeof types.ChatStreamDocument,
};
const documents: Documents = {
    "\n  mutation ClusterEnabledSet($id: ObjectID!, $enabled: Boolean!) {\n    clusterEnabledSet(id: $id, enabled: $enabled) {\n      id\n      spec {\n        enabled\n      }\n    }\n  }\n": types.ClusterEnabledSetDocument,
    "\n  mutation ClusterSyncEnabledSet($id: ObjectID!, $syncEnabled: Boolean!) {\n    clusterSyncEnabledSet(id: $id, syncEnabled: $syncEnabled) {\n      id\n      spec {\n        syncEnabled\n      }\n    }\n  }\n": types.ClusterSyncEnabledSetDocument,
    "\n  mutation ClusterCacheClear($id: ObjectID!) {\n    clusterCacheClear(id: $id) {\n      id\n    }\n  }\n": types.ClusterCacheClearDocument,
    "\n  mutation ClusterCachedKindSyncEnabledSet($id: ObjectID!, $syncEnabled: Boolean!) {\n    clusterCachedKindSyncEnabledSet(id: $id, syncEnabled: $syncEnabled) {\n      id\n      spec {\n        syncEnabled\n      }\n    }\n  }\n": types.ClusterCachedKindSyncEnabledSetDocument,
    "\n  mutation ClusterDelete($id: ObjectID!) {\n    clusterDelete(id: $id)\n  }\n": types.ClusterDeleteDocument,
    "\n  mutation ClusterConnectionRetry($id: ObjectID!) {\n    clusterConnectionRetry(id: $id)\n  }\n": types.ClusterConnectionRetryDocument,
    "\n  subscription ClusterConnectionEvents($id: ObjectID!) {\n    eventsWatch(id: $id, category: \"connection\") {\n      type\n      event {\n        id\n        type\n        reason\n        message\n        count\n        firstAt\n        lastAt\n      }\n    }\n  }\n": types.ClusterConnectionEventsDocument,
    "\n  subscription ClusterSyncEvents($id: ObjectID!) {\n    eventsWatch(id: $id, category: \"sync\") {\n      type\n      event {\n        id\n        type\n        reason\n        message\n        count\n        firstAt\n        lastAt\n      }\n    }\n  }\n": types.ClusterSyncEventsDocument,
    "\n  subscription ClusterDiscoveryEvents($id: ObjectID!) {\n    eventsWatch(id: $id, category: \"discovery\") {\n      type\n      event {\n        id\n        type\n        reason\n        message\n        count\n        firstAt\n        lastAt\n      }\n    }\n  }\n": types.ClusterDiscoveryEventsDocument,
    "\n  subscription ClusterCacheSyncStatus($id: ObjectID!, $cacheID: ObjectID!) {\n    clusterCacheSyncStatusWatch(id: $id, cacheID: $cacheID) {\n      discovery {\n        reason\n        message\n      }\n      kinds {\n        apiVersion\n        resource\n        reason\n        message\n        objectCount\n      }\n    }\n  }\n": types.ClusterCacheSyncStatusDocument,
    "\n  subscription ClusterCacheStats($id: ObjectID!, $cacheID: ObjectID!) {\n    clusterCacheStatsWatch(id: $id, cacheID: $cacheID) {\n      exists\n      bytes\n      dbBytes\n      walBytes\n      shmBytes\n      objectCount\n      kindCount\n    }\n  }\n": types.ClusterCacheStatsDocument,
    "\n  subscription ClusterCachedKinds($cacheID: ObjectID!) {\n    clusterCachedKindsWatch(cacheID: $cacheID) {\n      type\n      kind {\n        id\n        spec {\n          apiVersion\n          resource\n        }\n      }\n    }\n  }\n": types.ClusterCachedKindsDocument,
    "\n  subscription ClusterSchedule($id: ObjectID!) {\n    clusterScheduleWatch(id: $id) {\n      nextRequeueAt\n      probing\n    }\n  }\n": types.ClusterScheduleDocument,
    "\n  subscription AuthStateWatch {\n    authStateWatch {\n      authenticated\n      identity {\n        sub\n        email\n        name\n      }\n    }\n  }\n": types.AuthStateWatchDocument,
    "\n  mutation AuthLoginStart {\n    authLoginStart\n  }\n": types.AuthLoginStartDocument,
    "\n  mutation AuthLogout {\n    authLogout\n  }\n": types.AuthLogoutDocument,
    "\n  subscription ClusterCachedDataEventsWatch($id: ObjectID!, $cacheID: ObjectID!) {\n    clusterCachedDataEventsWatch(id: $id, cacheID: $cacheID) {\n      type\n      cacheID\n      event {\n        uid\n        type\n        reason\n        message\n        count\n        firstSeen\n        lastSeen\n        involvedKind\n        involvedNamespace\n        involvedName\n      }\n    }\n  }\n": types.ClusterCachedDataEventsWatchDocument,
    "\n  subscription ClusterCachedDataKindsWatch($id: ObjectID!, $cacheID: ObjectID!) {\n    clusterCachedDataKindsWatch(id: $id, cacheID: $cacheID) {\n      type\n      cacheID\n      kind {\n        apiVersion\n        kind\n        resource\n        scope\n        isCRD\n        count\n        printerColumns {\n          name\n          type\n          jsonPath\n          priority\n        }\n      }\n    }\n  }\n": types.ClusterCachedDataKindsWatchDocument,
    "\n  subscription ClusterCachedDataObjectsWatch(\n    $id: ObjectID!\n    $cacheID: ObjectID!\n    $apiVersion: String!\n    $resource: String!\n  ) {\n    clusterCachedDataObjectsWatch(id: $id, cacheID: $cacheID, apiVersion: $apiVersion, resource: $resource) {\n      type\n      cacheID\n      apiVersion\n      resource\n      object {\n        uid\n        apiVersion\n        kind\n        namespace\n        name\n        creationTimestamp\n        rawJSON\n      }\n    }\n  }\n": types.ClusterCachedDataObjectsWatchDocument,
    "\n  subscription ClustersWatch {\n    clustersWatch {\n      type\n      cluster {\n        id\n        deletionRequestedAt\n        spec {\n          name\n          syncEnabled\n          enabled\n          source {\n            kubeconfig {\n              context\n            }\n          }\n        }\n        status {\n          source {\n            kubeconfig {\n              cluster\n              user\n              isPresent\n              isDefault\n            }\n          }\n          server {\n            uid\n          }\n        }\n        conditions {\n          type\n          status\n          reason\n          message\n          liveness\n          unconfirmed\n          transitionedAt\n        }\n      }\n    }\n  }\n": types.ClustersWatchDocument,
    "\n  subscription ClusterCachesWatch {\n    clusterCachesWatch {\n      type\n      cache {\n        id\n        owner {\n          id\n        }\n        spec {\n          serverUid\n        }\n        # On-disk stats ride clusterCacheStatsWatch, subscribed per expanded row.\n      }\n    }\n  }\n": types.ClusterCachesWatchDocument,
    "\n  subscription ClusterCacheHealthWatch {\n    clusterCacheHealthWatch {\n      cacheID\n      status\n      reason\n      unhealthyKindRefs {\n        apiVersion\n        resource\n      }\n      totalKinds\n      unhealthyKinds\n      pausedKinds\n      lastUpdateAt\n      lastLiveAt\n    }\n  }\n": types.ClusterCacheHealthWatchDocument,
    "\n  subscription ChatStream($input: ChatInput!) {\n    chatStream(input: $input) {\n      delta\n      done\n    }\n  }\n": types.ChatStreamDocument,
};

/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 *
 *
 * @example
 * ```ts
 * const query = graphql(`query GetUser($id: ID!) { user(id: $id) { name } }`);
 * ```
 *
 * The query argument is unknown!
 * Please regenerate the types.
 */
export function graphql(source: string): unknown;

/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  mutation ClusterEnabledSet($id: ObjectID!, $enabled: Boolean!) {\n    clusterEnabledSet(id: $id, enabled: $enabled) {\n      id\n      spec {\n        enabled\n      }\n    }\n  }\n"): (typeof documents)["\n  mutation ClusterEnabledSet($id: ObjectID!, $enabled: Boolean!) {\n    clusterEnabledSet(id: $id, enabled: $enabled) {\n      id\n      spec {\n        enabled\n      }\n    }\n  }\n"];
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  mutation ClusterSyncEnabledSet($id: ObjectID!, $syncEnabled: Boolean!) {\n    clusterSyncEnabledSet(id: $id, syncEnabled: $syncEnabled) {\n      id\n      spec {\n        syncEnabled\n      }\n    }\n  }\n"): (typeof documents)["\n  mutation ClusterSyncEnabledSet($id: ObjectID!, $syncEnabled: Boolean!) {\n    clusterSyncEnabledSet(id: $id, syncEnabled: $syncEnabled) {\n      id\n      spec {\n        syncEnabled\n      }\n    }\n  }\n"];
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  mutation ClusterCacheClear($id: ObjectID!) {\n    clusterCacheClear(id: $id) {\n      id\n    }\n  }\n"): (typeof documents)["\n  mutation ClusterCacheClear($id: ObjectID!) {\n    clusterCacheClear(id: $id) {\n      id\n    }\n  }\n"];
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  mutation ClusterCachedKindSyncEnabledSet($id: ObjectID!, $syncEnabled: Boolean!) {\n    clusterCachedKindSyncEnabledSet(id: $id, syncEnabled: $syncEnabled) {\n      id\n      spec {\n        syncEnabled\n      }\n    }\n  }\n"): (typeof documents)["\n  mutation ClusterCachedKindSyncEnabledSet($id: ObjectID!, $syncEnabled: Boolean!) {\n    clusterCachedKindSyncEnabledSet(id: $id, syncEnabled: $syncEnabled) {\n      id\n      spec {\n        syncEnabled\n      }\n    }\n  }\n"];
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  mutation ClusterDelete($id: ObjectID!) {\n    clusterDelete(id: $id)\n  }\n"): (typeof documents)["\n  mutation ClusterDelete($id: ObjectID!) {\n    clusterDelete(id: $id)\n  }\n"];
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  mutation ClusterConnectionRetry($id: ObjectID!) {\n    clusterConnectionRetry(id: $id)\n  }\n"): (typeof documents)["\n  mutation ClusterConnectionRetry($id: ObjectID!) {\n    clusterConnectionRetry(id: $id)\n  }\n"];
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  subscription ClusterConnectionEvents($id: ObjectID!) {\n    eventsWatch(id: $id, category: \"connection\") {\n      type\n      event {\n        id\n        type\n        reason\n        message\n        count\n        firstAt\n        lastAt\n      }\n    }\n  }\n"): (typeof documents)["\n  subscription ClusterConnectionEvents($id: ObjectID!) {\n    eventsWatch(id: $id, category: \"connection\") {\n      type\n      event {\n        id\n        type\n        reason\n        message\n        count\n        firstAt\n        lastAt\n      }\n    }\n  }\n"];
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  subscription ClusterSyncEvents($id: ObjectID!) {\n    eventsWatch(id: $id, category: \"sync\") {\n      type\n      event {\n        id\n        type\n        reason\n        message\n        count\n        firstAt\n        lastAt\n      }\n    }\n  }\n"): (typeof documents)["\n  subscription ClusterSyncEvents($id: ObjectID!) {\n    eventsWatch(id: $id, category: \"sync\") {\n      type\n      event {\n        id\n        type\n        reason\n        message\n        count\n        firstAt\n        lastAt\n      }\n    }\n  }\n"];
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  subscription ClusterDiscoveryEvents($id: ObjectID!) {\n    eventsWatch(id: $id, category: \"discovery\") {\n      type\n      event {\n        id\n        type\n        reason\n        message\n        count\n        firstAt\n        lastAt\n      }\n    }\n  }\n"): (typeof documents)["\n  subscription ClusterDiscoveryEvents($id: ObjectID!) {\n    eventsWatch(id: $id, category: \"discovery\") {\n      type\n      event {\n        id\n        type\n        reason\n        message\n        count\n        firstAt\n        lastAt\n      }\n    }\n  }\n"];
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  subscription ClusterCacheSyncStatus($id: ObjectID!, $cacheID: ObjectID!) {\n    clusterCacheSyncStatusWatch(id: $id, cacheID: $cacheID) {\n      discovery {\n        reason\n        message\n      }\n      kinds {\n        apiVersion\n        resource\n        reason\n        message\n        objectCount\n      }\n    }\n  }\n"): (typeof documents)["\n  subscription ClusterCacheSyncStatus($id: ObjectID!, $cacheID: ObjectID!) {\n    clusterCacheSyncStatusWatch(id: $id, cacheID: $cacheID) {\n      discovery {\n        reason\n        message\n      }\n      kinds {\n        apiVersion\n        resource\n        reason\n        message\n        objectCount\n      }\n    }\n  }\n"];
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  subscription ClusterCacheStats($id: ObjectID!, $cacheID: ObjectID!) {\n    clusterCacheStatsWatch(id: $id, cacheID: $cacheID) {\n      exists\n      bytes\n      dbBytes\n      walBytes\n      shmBytes\n      objectCount\n      kindCount\n    }\n  }\n"): (typeof documents)["\n  subscription ClusterCacheStats($id: ObjectID!, $cacheID: ObjectID!) {\n    clusterCacheStatsWatch(id: $id, cacheID: $cacheID) {\n      exists\n      bytes\n      dbBytes\n      walBytes\n      shmBytes\n      objectCount\n      kindCount\n    }\n  }\n"];
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  subscription ClusterCachedKinds($cacheID: ObjectID!) {\n    clusterCachedKindsWatch(cacheID: $cacheID) {\n      type\n      kind {\n        id\n        spec {\n          apiVersion\n          resource\n        }\n      }\n    }\n  }\n"): (typeof documents)["\n  subscription ClusterCachedKinds($cacheID: ObjectID!) {\n    clusterCachedKindsWatch(cacheID: $cacheID) {\n      type\n      kind {\n        id\n        spec {\n          apiVersion\n          resource\n        }\n      }\n    }\n  }\n"];
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  subscription ClusterSchedule($id: ObjectID!) {\n    clusterScheduleWatch(id: $id) {\n      nextRequeueAt\n      probing\n    }\n  }\n"): (typeof documents)["\n  subscription ClusterSchedule($id: ObjectID!) {\n    clusterScheduleWatch(id: $id) {\n      nextRequeueAt\n      probing\n    }\n  }\n"];
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  subscription AuthStateWatch {\n    authStateWatch {\n      authenticated\n      identity {\n        sub\n        email\n        name\n      }\n    }\n  }\n"): (typeof documents)["\n  subscription AuthStateWatch {\n    authStateWatch {\n      authenticated\n      identity {\n        sub\n        email\n        name\n      }\n    }\n  }\n"];
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  mutation AuthLoginStart {\n    authLoginStart\n  }\n"): (typeof documents)["\n  mutation AuthLoginStart {\n    authLoginStart\n  }\n"];
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  mutation AuthLogout {\n    authLogout\n  }\n"): (typeof documents)["\n  mutation AuthLogout {\n    authLogout\n  }\n"];
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  subscription ClusterCachedDataEventsWatch($id: ObjectID!, $cacheID: ObjectID!) {\n    clusterCachedDataEventsWatch(id: $id, cacheID: $cacheID) {\n      type\n      cacheID\n      event {\n        uid\n        type\n        reason\n        message\n        count\n        firstSeen\n        lastSeen\n        involvedKind\n        involvedNamespace\n        involvedName\n      }\n    }\n  }\n"): (typeof documents)["\n  subscription ClusterCachedDataEventsWatch($id: ObjectID!, $cacheID: ObjectID!) {\n    clusterCachedDataEventsWatch(id: $id, cacheID: $cacheID) {\n      type\n      cacheID\n      event {\n        uid\n        type\n        reason\n        message\n        count\n        firstSeen\n        lastSeen\n        involvedKind\n        involvedNamespace\n        involvedName\n      }\n    }\n  }\n"];
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  subscription ClusterCachedDataKindsWatch($id: ObjectID!, $cacheID: ObjectID!) {\n    clusterCachedDataKindsWatch(id: $id, cacheID: $cacheID) {\n      type\n      cacheID\n      kind {\n        apiVersion\n        kind\n        resource\n        scope\n        isCRD\n        count\n        printerColumns {\n          name\n          type\n          jsonPath\n          priority\n        }\n      }\n    }\n  }\n"): (typeof documents)["\n  subscription ClusterCachedDataKindsWatch($id: ObjectID!, $cacheID: ObjectID!) {\n    clusterCachedDataKindsWatch(id: $id, cacheID: $cacheID) {\n      type\n      cacheID\n      kind {\n        apiVersion\n        kind\n        resource\n        scope\n        isCRD\n        count\n        printerColumns {\n          name\n          type\n          jsonPath\n          priority\n        }\n      }\n    }\n  }\n"];
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  subscription ClusterCachedDataObjectsWatch(\n    $id: ObjectID!\n    $cacheID: ObjectID!\n    $apiVersion: String!\n    $resource: String!\n  ) {\n    clusterCachedDataObjectsWatch(id: $id, cacheID: $cacheID, apiVersion: $apiVersion, resource: $resource) {\n      type\n      cacheID\n      apiVersion\n      resource\n      object {\n        uid\n        apiVersion\n        kind\n        namespace\n        name\n        creationTimestamp\n        rawJSON\n      }\n    }\n  }\n"): (typeof documents)["\n  subscription ClusterCachedDataObjectsWatch(\n    $id: ObjectID!\n    $cacheID: ObjectID!\n    $apiVersion: String!\n    $resource: String!\n  ) {\n    clusterCachedDataObjectsWatch(id: $id, cacheID: $cacheID, apiVersion: $apiVersion, resource: $resource) {\n      type\n      cacheID\n      apiVersion\n      resource\n      object {\n        uid\n        apiVersion\n        kind\n        namespace\n        name\n        creationTimestamp\n        rawJSON\n      }\n    }\n  }\n"];
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  subscription ClustersWatch {\n    clustersWatch {\n      type\n      cluster {\n        id\n        deletionRequestedAt\n        spec {\n          name\n          syncEnabled\n          enabled\n          source {\n            kubeconfig {\n              context\n            }\n          }\n        }\n        status {\n          source {\n            kubeconfig {\n              cluster\n              user\n              isPresent\n              isDefault\n            }\n          }\n          server {\n            uid\n          }\n        }\n        conditions {\n          type\n          status\n          reason\n          message\n          liveness\n          unconfirmed\n          transitionedAt\n        }\n      }\n    }\n  }\n"): (typeof documents)["\n  subscription ClustersWatch {\n    clustersWatch {\n      type\n      cluster {\n        id\n        deletionRequestedAt\n        spec {\n          name\n          syncEnabled\n          enabled\n          source {\n            kubeconfig {\n              context\n            }\n          }\n        }\n        status {\n          source {\n            kubeconfig {\n              cluster\n              user\n              isPresent\n              isDefault\n            }\n          }\n          server {\n            uid\n          }\n        }\n        conditions {\n          type\n          status\n          reason\n          message\n          liveness\n          unconfirmed\n          transitionedAt\n        }\n      }\n    }\n  }\n"];
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  subscription ClusterCachesWatch {\n    clusterCachesWatch {\n      type\n      cache {\n        id\n        owner {\n          id\n        }\n        spec {\n          serverUid\n        }\n        # On-disk stats ride clusterCacheStatsWatch, subscribed per expanded row.\n      }\n    }\n  }\n"): (typeof documents)["\n  subscription ClusterCachesWatch {\n    clusterCachesWatch {\n      type\n      cache {\n        id\n        owner {\n          id\n        }\n        spec {\n          serverUid\n        }\n        # On-disk stats ride clusterCacheStatsWatch, subscribed per expanded row.\n      }\n    }\n  }\n"];
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  subscription ClusterCacheHealthWatch {\n    clusterCacheHealthWatch {\n      cacheID\n      status\n      reason\n      unhealthyKindRefs {\n        apiVersion\n        resource\n      }\n      totalKinds\n      unhealthyKinds\n      pausedKinds\n      lastUpdateAt\n      lastLiveAt\n    }\n  }\n"): (typeof documents)["\n  subscription ClusterCacheHealthWatch {\n    clusterCacheHealthWatch {\n      cacheID\n      status\n      reason\n      unhealthyKindRefs {\n        apiVersion\n        resource\n      }\n      totalKinds\n      unhealthyKinds\n      pausedKinds\n      lastUpdateAt\n      lastLiveAt\n    }\n  }\n"];
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  subscription ChatStream($input: ChatInput!) {\n    chatStream(input: $input) {\n      delta\n      done\n    }\n  }\n"): (typeof documents)["\n  subscription ChatStream($input: ChatInput!) {\n    chatStream(input: $input) {\n      delta\n      done\n    }\n  }\n"];

export function graphql(source: string) {
  return (documents as any)[source] ?? {};
}

export type DocumentType<TDocumentNode extends DocumentNode<any, any>> = TDocumentNode extends DocumentNode<  infer TType,  any>  ? TType  : never;