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
    "\n  mutation ClusterDelete($id: ObjectID!) {\n    clusterDelete(id: $id)\n  }\n": typeof types.ClusterDeleteDocument,
    "\n  mutation ClusterConnectionRetry($id: ObjectID!) {\n    clusterConnectionRetry(id: $id)\n  }\n": typeof types.ClusterConnectionRetryDocument,
    "\n  subscription ClusterConnectionEvents($id: ObjectID!) {\n    clusterEventsWatch(id: $id, category: \"connection\") {\n      type\n      event {\n        id\n        type\n        reason\n        message\n        count\n        firstAt\n        lastAt\n      }\n    }\n  }\n": typeof types.ClusterConnectionEventsDocument,
    "\n  subscription ClusterSyncEvents($id: ObjectID!) {\n    clusterCacheGVRSyncEventsWatch(id: $id, category: \"sync\") {\n      type\n      event {\n        id\n        type\n        reason\n        message\n        count\n        firstAt\n        lastAt\n      }\n    }\n  }\n": typeof types.ClusterSyncEventsDocument,
    "\n  subscription ClusterCacheGVRDiscoveries {\n    clusterCacheGVRDiscoveriesWatch {\n      type\n      discovery {\n        id\n        cacheID\n        stats {\n          lastDiscoveryAt\n          resourceCount\n        }\n        conditions {\n          type\n          reason\n          message\n          unconfirmed\n        }\n      }\n    }\n  }\n": typeof types.ClusterCacheGvrDiscoveriesDocument,
    "\n  subscription ClusterCacheStats($id: ObjectID!, $cacheID: ObjectID!) {\n    clusterCacheStatsWatch(id: $id, cacheID: $cacheID) {\n      exists\n      bytes\n      objectCount\n      kindCount\n    }\n  }\n": typeof types.ClusterCacheStatsDocument,
    "\n  subscription ClusterCacheGVRSyncs($cacheID: ObjectID!) {\n    clusterCacheGVRSyncsWatch(cacheID: $cacheID) {\n      type\n      sync {\n        id\n        spec {\n          apiVersion\n          resource\n        }\n      }\n    }\n  }\n": typeof types.ClusterCacheGvrSyncsDocument,
    "\n  subscription ClusterSchedule($id: ObjectID!) {\n    clusterScheduleWatch(id: $id) {\n      nextRequeueAt\n      probing\n    }\n  }\n": typeof types.ClusterScheduleDocument,
    "\n  subscription AuthStateWatch {\n    authStateWatch {\n      authenticated\n      identity {\n        sub\n        email\n        name\n      }\n    }\n  }\n": typeof types.AuthStateWatchDocument,
    "\n  mutation AuthLoginStart {\n    authLoginStart\n  }\n": typeof types.AuthLoginStartDocument,
    "\n  mutation AuthLogout {\n    authLogout\n  }\n": typeof types.AuthLogoutDocument,
    "\n  subscription ClusterDataEventsWatch($id: ObjectID!, $cacheID: ObjectID!) {\n    clusterDataEventsWatch(id: $id, cacheID: $cacheID) {\n      type\n      cacheID\n      event {\n        uid\n        type\n        reason\n        message\n        count\n        firstSeen\n        lastSeen\n        involvedKind\n        involvedNamespace\n        involvedName\n      }\n    }\n  }\n": typeof types.ClusterDataEventsWatchDocument,
    "\n  subscription ClusterDataKindsWatch($id: ObjectID!, $cacheID: ObjectID!) {\n    clusterDataKindsWatch(id: $id, cacheID: $cacheID) {\n      type\n      cacheID\n      kind {\n        apiVersion\n        kind\n        resource\n        scope\n        isCRD\n        count\n      }\n    }\n  }\n": typeof types.ClusterDataKindsWatchDocument,
    "\n  subscription ClusterDataObjectsWatch($id: ObjectID!, $cacheID: ObjectID!, $apiVersion: String!, $resource: String!) {\n    clusterDataObjectsWatch(id: $id, cacheID: $cacheID, apiVersion: $apiVersion, resource: $resource) {\n      type\n      cacheID\n      apiVersion\n      resource\n      object {\n        uid\n        apiVersion\n        kind\n        namespace\n        name\n        creationTimestamp\n        rawJSON\n      }\n    }\n  }\n": typeof types.ClusterDataObjectsWatchDocument,
    "\n  subscription ClustersWatch {\n    clustersWatch {\n      type\n      cluster {\n        id\n        spec {\n          name\n          syncEnabled\n          enabled\n          source {\n            kubeconfig {\n              context\n            }\n          }\n        }\n        status {\n          source {\n            kubeconfig {\n              cluster\n              user\n              isPresent\n              isDefault\n            }\n          }\n          server {\n            uid\n          }\n          lastConnectedAt\n        }\n        conditions {\n          type\n          status\n          reason\n          message\n          liveness\n          unconfirmed\n          transitionedAt\n        }\n      }\n    }\n  }\n": typeof types.ClustersWatchDocument,
    "\n  subscription ClusterCachesWatch {\n    clusterCachesWatch {\n      type\n      cache {\n        id\n        clusterID\n        serverUid\n        # No stats: whether a cache file exists, its size and its object/kind counts all\n        # ride clusterCacheStatsWatch, since a settled cache's record stops changing and a\n        # field here would freeze at whatever the cache held when the window subscribed.\n        # Each stats field is also a resolver call (a filesystem stat plus a kind_counts\n        # read) per cache on every frame of this always-mounted stream.\n      }\n    }\n  }\n": typeof types.ClusterCachesWatchDocument,
    "\n  subscription ClusterCacheSyncHealthWatch {\n    clusterCacheSyncHealthWatch {\n      cacheID\n      status\n      reason\n      unhealthyKindRefs {\n        apiVersion\n        resource\n      }\n      totalKinds\n      unhealthyKinds\n      lastUpdateAt\n      lastLiveAt\n    }\n  }\n": typeof types.ClusterCacheSyncHealthWatchDocument,
    "\n  subscription ChatStream($input: ChatInput!) {\n    chatStream(input: $input) {\n      delta\n      done\n    }\n  }\n": typeof types.ChatStreamDocument,
};
const documents: Documents = {
    "\n  mutation ClusterEnabledSet($id: ObjectID!, $enabled: Boolean!) {\n    clusterEnabledSet(id: $id, enabled: $enabled) {\n      id\n      spec {\n        enabled\n      }\n    }\n  }\n": types.ClusterEnabledSetDocument,
    "\n  mutation ClusterSyncEnabledSet($id: ObjectID!, $syncEnabled: Boolean!) {\n    clusterSyncEnabledSet(id: $id, syncEnabled: $syncEnabled) {\n      id\n      spec {\n        syncEnabled\n      }\n    }\n  }\n": types.ClusterSyncEnabledSetDocument,
    "\n  mutation ClusterCacheClear($id: ObjectID!) {\n    clusterCacheClear(id: $id) {\n      id\n    }\n  }\n": types.ClusterCacheClearDocument,
    "\n  mutation ClusterDelete($id: ObjectID!) {\n    clusterDelete(id: $id)\n  }\n": types.ClusterDeleteDocument,
    "\n  mutation ClusterConnectionRetry($id: ObjectID!) {\n    clusterConnectionRetry(id: $id)\n  }\n": types.ClusterConnectionRetryDocument,
    "\n  subscription ClusterConnectionEvents($id: ObjectID!) {\n    clusterEventsWatch(id: $id, category: \"connection\") {\n      type\n      event {\n        id\n        type\n        reason\n        message\n        count\n        firstAt\n        lastAt\n      }\n    }\n  }\n": types.ClusterConnectionEventsDocument,
    "\n  subscription ClusterSyncEvents($id: ObjectID!) {\n    clusterCacheGVRSyncEventsWatch(id: $id, category: \"sync\") {\n      type\n      event {\n        id\n        type\n        reason\n        message\n        count\n        firstAt\n        lastAt\n      }\n    }\n  }\n": types.ClusterSyncEventsDocument,
    "\n  subscription ClusterCacheGVRDiscoveries {\n    clusterCacheGVRDiscoveriesWatch {\n      type\n      discovery {\n        id\n        cacheID\n        stats {\n          lastDiscoveryAt\n          resourceCount\n        }\n        conditions {\n          type\n          reason\n          message\n          unconfirmed\n        }\n      }\n    }\n  }\n": types.ClusterCacheGvrDiscoveriesDocument,
    "\n  subscription ClusterCacheStats($id: ObjectID!, $cacheID: ObjectID!) {\n    clusterCacheStatsWatch(id: $id, cacheID: $cacheID) {\n      exists\n      bytes\n      objectCount\n      kindCount\n    }\n  }\n": types.ClusterCacheStatsDocument,
    "\n  subscription ClusterCacheGVRSyncs($cacheID: ObjectID!) {\n    clusterCacheGVRSyncsWatch(cacheID: $cacheID) {\n      type\n      sync {\n        id\n        spec {\n          apiVersion\n          resource\n        }\n      }\n    }\n  }\n": types.ClusterCacheGvrSyncsDocument,
    "\n  subscription ClusterSchedule($id: ObjectID!) {\n    clusterScheduleWatch(id: $id) {\n      nextRequeueAt\n      probing\n    }\n  }\n": types.ClusterScheduleDocument,
    "\n  subscription AuthStateWatch {\n    authStateWatch {\n      authenticated\n      identity {\n        sub\n        email\n        name\n      }\n    }\n  }\n": types.AuthStateWatchDocument,
    "\n  mutation AuthLoginStart {\n    authLoginStart\n  }\n": types.AuthLoginStartDocument,
    "\n  mutation AuthLogout {\n    authLogout\n  }\n": types.AuthLogoutDocument,
    "\n  subscription ClusterDataEventsWatch($id: ObjectID!, $cacheID: ObjectID!) {\n    clusterDataEventsWatch(id: $id, cacheID: $cacheID) {\n      type\n      cacheID\n      event {\n        uid\n        type\n        reason\n        message\n        count\n        firstSeen\n        lastSeen\n        involvedKind\n        involvedNamespace\n        involvedName\n      }\n    }\n  }\n": types.ClusterDataEventsWatchDocument,
    "\n  subscription ClusterDataKindsWatch($id: ObjectID!, $cacheID: ObjectID!) {\n    clusterDataKindsWatch(id: $id, cacheID: $cacheID) {\n      type\n      cacheID\n      kind {\n        apiVersion\n        kind\n        resource\n        scope\n        isCRD\n        count\n      }\n    }\n  }\n": types.ClusterDataKindsWatchDocument,
    "\n  subscription ClusterDataObjectsWatch($id: ObjectID!, $cacheID: ObjectID!, $apiVersion: String!, $resource: String!) {\n    clusterDataObjectsWatch(id: $id, cacheID: $cacheID, apiVersion: $apiVersion, resource: $resource) {\n      type\n      cacheID\n      apiVersion\n      resource\n      object {\n        uid\n        apiVersion\n        kind\n        namespace\n        name\n        creationTimestamp\n        rawJSON\n      }\n    }\n  }\n": types.ClusterDataObjectsWatchDocument,
    "\n  subscription ClustersWatch {\n    clustersWatch {\n      type\n      cluster {\n        id\n        spec {\n          name\n          syncEnabled\n          enabled\n          source {\n            kubeconfig {\n              context\n            }\n          }\n        }\n        status {\n          source {\n            kubeconfig {\n              cluster\n              user\n              isPresent\n              isDefault\n            }\n          }\n          server {\n            uid\n          }\n          lastConnectedAt\n        }\n        conditions {\n          type\n          status\n          reason\n          message\n          liveness\n          unconfirmed\n          transitionedAt\n        }\n      }\n    }\n  }\n": types.ClustersWatchDocument,
    "\n  subscription ClusterCachesWatch {\n    clusterCachesWatch {\n      type\n      cache {\n        id\n        clusterID\n        serverUid\n        # No stats: whether a cache file exists, its size and its object/kind counts all\n        # ride clusterCacheStatsWatch, since a settled cache's record stops changing and a\n        # field here would freeze at whatever the cache held when the window subscribed.\n        # Each stats field is also a resolver call (a filesystem stat plus a kind_counts\n        # read) per cache on every frame of this always-mounted stream.\n      }\n    }\n  }\n": types.ClusterCachesWatchDocument,
    "\n  subscription ClusterCacheSyncHealthWatch {\n    clusterCacheSyncHealthWatch {\n      cacheID\n      status\n      reason\n      unhealthyKindRefs {\n        apiVersion\n        resource\n      }\n      totalKinds\n      unhealthyKinds\n      lastUpdateAt\n      lastLiveAt\n    }\n  }\n": types.ClusterCacheSyncHealthWatchDocument,
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
export function graphql(source: "\n  mutation ClusterDelete($id: ObjectID!) {\n    clusterDelete(id: $id)\n  }\n"): (typeof documents)["\n  mutation ClusterDelete($id: ObjectID!) {\n    clusterDelete(id: $id)\n  }\n"];
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  mutation ClusterConnectionRetry($id: ObjectID!) {\n    clusterConnectionRetry(id: $id)\n  }\n"): (typeof documents)["\n  mutation ClusterConnectionRetry($id: ObjectID!) {\n    clusterConnectionRetry(id: $id)\n  }\n"];
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  subscription ClusterConnectionEvents($id: ObjectID!) {\n    clusterEventsWatch(id: $id, category: \"connection\") {\n      type\n      event {\n        id\n        type\n        reason\n        message\n        count\n        firstAt\n        lastAt\n      }\n    }\n  }\n"): (typeof documents)["\n  subscription ClusterConnectionEvents($id: ObjectID!) {\n    clusterEventsWatch(id: $id, category: \"connection\") {\n      type\n      event {\n        id\n        type\n        reason\n        message\n        count\n        firstAt\n        lastAt\n      }\n    }\n  }\n"];
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  subscription ClusterSyncEvents($id: ObjectID!) {\n    clusterCacheGVRSyncEventsWatch(id: $id, category: \"sync\") {\n      type\n      event {\n        id\n        type\n        reason\n        message\n        count\n        firstAt\n        lastAt\n      }\n    }\n  }\n"): (typeof documents)["\n  subscription ClusterSyncEvents($id: ObjectID!) {\n    clusterCacheGVRSyncEventsWatch(id: $id, category: \"sync\") {\n      type\n      event {\n        id\n        type\n        reason\n        message\n        count\n        firstAt\n        lastAt\n      }\n    }\n  }\n"];
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  subscription ClusterCacheGVRDiscoveries {\n    clusterCacheGVRDiscoveriesWatch {\n      type\n      discovery {\n        id\n        cacheID\n        stats {\n          lastDiscoveryAt\n          resourceCount\n        }\n        conditions {\n          type\n          reason\n          message\n          unconfirmed\n        }\n      }\n    }\n  }\n"): (typeof documents)["\n  subscription ClusterCacheGVRDiscoveries {\n    clusterCacheGVRDiscoveriesWatch {\n      type\n      discovery {\n        id\n        cacheID\n        stats {\n          lastDiscoveryAt\n          resourceCount\n        }\n        conditions {\n          type\n          reason\n          message\n          unconfirmed\n        }\n      }\n    }\n  }\n"];
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  subscription ClusterCacheStats($id: ObjectID!, $cacheID: ObjectID!) {\n    clusterCacheStatsWatch(id: $id, cacheID: $cacheID) {\n      exists\n      bytes\n      objectCount\n      kindCount\n    }\n  }\n"): (typeof documents)["\n  subscription ClusterCacheStats($id: ObjectID!, $cacheID: ObjectID!) {\n    clusterCacheStatsWatch(id: $id, cacheID: $cacheID) {\n      exists\n      bytes\n      objectCount\n      kindCount\n    }\n  }\n"];
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  subscription ClusterCacheGVRSyncs($cacheID: ObjectID!) {\n    clusterCacheGVRSyncsWatch(cacheID: $cacheID) {\n      type\n      sync {\n        id\n        spec {\n          apiVersion\n          resource\n        }\n      }\n    }\n  }\n"): (typeof documents)["\n  subscription ClusterCacheGVRSyncs($cacheID: ObjectID!) {\n    clusterCacheGVRSyncsWatch(cacheID: $cacheID) {\n      type\n      sync {\n        id\n        spec {\n          apiVersion\n          resource\n        }\n      }\n    }\n  }\n"];
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
export function graphql(source: "\n  subscription ClusterDataEventsWatch($id: ObjectID!, $cacheID: ObjectID!) {\n    clusterDataEventsWatch(id: $id, cacheID: $cacheID) {\n      type\n      cacheID\n      event {\n        uid\n        type\n        reason\n        message\n        count\n        firstSeen\n        lastSeen\n        involvedKind\n        involvedNamespace\n        involvedName\n      }\n    }\n  }\n"): (typeof documents)["\n  subscription ClusterDataEventsWatch($id: ObjectID!, $cacheID: ObjectID!) {\n    clusterDataEventsWatch(id: $id, cacheID: $cacheID) {\n      type\n      cacheID\n      event {\n        uid\n        type\n        reason\n        message\n        count\n        firstSeen\n        lastSeen\n        involvedKind\n        involvedNamespace\n        involvedName\n      }\n    }\n  }\n"];
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  subscription ClusterDataKindsWatch($id: ObjectID!, $cacheID: ObjectID!) {\n    clusterDataKindsWatch(id: $id, cacheID: $cacheID) {\n      type\n      cacheID\n      kind {\n        apiVersion\n        kind\n        resource\n        scope\n        isCRD\n        count\n      }\n    }\n  }\n"): (typeof documents)["\n  subscription ClusterDataKindsWatch($id: ObjectID!, $cacheID: ObjectID!) {\n    clusterDataKindsWatch(id: $id, cacheID: $cacheID) {\n      type\n      cacheID\n      kind {\n        apiVersion\n        kind\n        resource\n        scope\n        isCRD\n        count\n      }\n    }\n  }\n"];
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  subscription ClusterDataObjectsWatch($id: ObjectID!, $cacheID: ObjectID!, $apiVersion: String!, $resource: String!) {\n    clusterDataObjectsWatch(id: $id, cacheID: $cacheID, apiVersion: $apiVersion, resource: $resource) {\n      type\n      cacheID\n      apiVersion\n      resource\n      object {\n        uid\n        apiVersion\n        kind\n        namespace\n        name\n        creationTimestamp\n        rawJSON\n      }\n    }\n  }\n"): (typeof documents)["\n  subscription ClusterDataObjectsWatch($id: ObjectID!, $cacheID: ObjectID!, $apiVersion: String!, $resource: String!) {\n    clusterDataObjectsWatch(id: $id, cacheID: $cacheID, apiVersion: $apiVersion, resource: $resource) {\n      type\n      cacheID\n      apiVersion\n      resource\n      object {\n        uid\n        apiVersion\n        kind\n        namespace\n        name\n        creationTimestamp\n        rawJSON\n      }\n    }\n  }\n"];
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  subscription ClustersWatch {\n    clustersWatch {\n      type\n      cluster {\n        id\n        spec {\n          name\n          syncEnabled\n          enabled\n          source {\n            kubeconfig {\n              context\n            }\n          }\n        }\n        status {\n          source {\n            kubeconfig {\n              cluster\n              user\n              isPresent\n              isDefault\n            }\n          }\n          server {\n            uid\n          }\n          lastConnectedAt\n        }\n        conditions {\n          type\n          status\n          reason\n          message\n          liveness\n          unconfirmed\n          transitionedAt\n        }\n      }\n    }\n  }\n"): (typeof documents)["\n  subscription ClustersWatch {\n    clustersWatch {\n      type\n      cluster {\n        id\n        spec {\n          name\n          syncEnabled\n          enabled\n          source {\n            kubeconfig {\n              context\n            }\n          }\n        }\n        status {\n          source {\n            kubeconfig {\n              cluster\n              user\n              isPresent\n              isDefault\n            }\n          }\n          server {\n            uid\n          }\n          lastConnectedAt\n        }\n        conditions {\n          type\n          status\n          reason\n          message\n          liveness\n          unconfirmed\n          transitionedAt\n        }\n      }\n    }\n  }\n"];
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  subscription ClusterCachesWatch {\n    clusterCachesWatch {\n      type\n      cache {\n        id\n        clusterID\n        serverUid\n        # No stats: whether a cache file exists, its size and its object/kind counts all\n        # ride clusterCacheStatsWatch, since a settled cache's record stops changing and a\n        # field here would freeze at whatever the cache held when the window subscribed.\n        # Each stats field is also a resolver call (a filesystem stat plus a kind_counts\n        # read) per cache on every frame of this always-mounted stream.\n      }\n    }\n  }\n"): (typeof documents)["\n  subscription ClusterCachesWatch {\n    clusterCachesWatch {\n      type\n      cache {\n        id\n        clusterID\n        serverUid\n        # No stats: whether a cache file exists, its size and its object/kind counts all\n        # ride clusterCacheStatsWatch, since a settled cache's record stops changing and a\n        # field here would freeze at whatever the cache held when the window subscribed.\n        # Each stats field is also a resolver call (a filesystem stat plus a kind_counts\n        # read) per cache on every frame of this always-mounted stream.\n      }\n    }\n  }\n"];
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  subscription ClusterCacheSyncHealthWatch {\n    clusterCacheSyncHealthWatch {\n      cacheID\n      status\n      reason\n      unhealthyKindRefs {\n        apiVersion\n        resource\n      }\n      totalKinds\n      unhealthyKinds\n      lastUpdateAt\n      lastLiveAt\n    }\n  }\n"): (typeof documents)["\n  subscription ClusterCacheSyncHealthWatch {\n    clusterCacheSyncHealthWatch {\n      cacheID\n      status\n      reason\n      unhealthyKindRefs {\n        apiVersion\n        resource\n      }\n      totalKinds\n      unhealthyKinds\n      lastUpdateAt\n      lastLiveAt\n    }\n  }\n"];
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  subscription ChatStream($input: ChatInput!) {\n    chatStream(input: $input) {\n      delta\n      done\n    }\n  }\n"): (typeof documents)["\n  subscription ChatStream($input: ChatInput!) {\n    chatStream(input: $input) {\n      delta\n      done\n    }\n  }\n"];

export function graphql(source: string) {
  return (documents as any)[source] ?? {};
}

export type DocumentType<TDocumentNode extends DocumentNode<any, any>> = TDocumentNode extends DocumentNode<  infer TType,  any>  ? TType  : never;