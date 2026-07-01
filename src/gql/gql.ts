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
    "\n  subscription AuthStateWatch {\n    authStateWatch {\n      authenticated\n      identity {\n        sub\n        email\n        name\n      }\n    }\n  }\n": typeof types.AuthStateWatchDocument,
    "\n  mutation AuthLoginStart {\n    authLoginStart\n  }\n": typeof types.AuthLoginStartDocument,
    "\n  mutation AuthLogout {\n    authLogout\n  }\n": typeof types.AuthLogoutDocument,
    "\n  subscription ClustersWatch {\n    clustersWatch {\n      id\n      spec {\n        name\n        syncEnabled\n        enabled\n        source {\n          kubeconfig {\n            context\n          }\n        }\n      }\n      status {\n        source {\n          kubeconfig {\n            cluster\n            user\n            isPresent\n            isDefault\n          }\n        }\n        server {\n          uid\n        }\n        lastConnectedAt\n        conditions {\n          type\n          status\n          reason\n          message\n          lastTransitionTime\n        }\n      }\n      activeCache {\n        id\n        serverUid\n        enabled\n        status {\n          conditions {\n            type\n            status\n            reason\n          }\n          lastSyncedAt\n        }\n        stats {\n          exists\n          bytes\n        }\n      }\n      nextAttemptAt\n      connectionAttempts {\n        ok\n        reason\n        message\n        count\n        firstAt\n        lastAt\n      }\n    }\n  }\n": typeof types.ClustersWatchDocument,
    "\n  subscription ChatStream($input: ChatInput!) {\n    chatStream(input: $input) {\n      delta\n      done\n    }\n  }\n": typeof types.ChatStreamDocument,
};
const documents: Documents = {
    "\n  mutation ClusterEnabledSet($id: ObjectID!, $enabled: Boolean!) {\n    clusterEnabledSet(id: $id, enabled: $enabled) {\n      id\n      spec {\n        enabled\n      }\n    }\n  }\n": types.ClusterEnabledSetDocument,
    "\n  mutation ClusterSyncEnabledSet($id: ObjectID!, $syncEnabled: Boolean!) {\n    clusterSyncEnabledSet(id: $id, syncEnabled: $syncEnabled) {\n      id\n      spec {\n        syncEnabled\n      }\n    }\n  }\n": types.ClusterSyncEnabledSetDocument,
    "\n  mutation ClusterCacheClear($id: ObjectID!) {\n    clusterCacheClear(id: $id) {\n      id\n    }\n  }\n": types.ClusterCacheClearDocument,
    "\n  mutation ClusterDelete($id: ObjectID!) {\n    clusterDelete(id: $id)\n  }\n": types.ClusterDeleteDocument,
    "\n  mutation ClusterConnectionRetry($id: ObjectID!) {\n    clusterConnectionRetry(id: $id)\n  }\n": types.ClusterConnectionRetryDocument,
    "\n  subscription AuthStateWatch {\n    authStateWatch {\n      authenticated\n      identity {\n        sub\n        email\n        name\n      }\n    }\n  }\n": types.AuthStateWatchDocument,
    "\n  mutation AuthLoginStart {\n    authLoginStart\n  }\n": types.AuthLoginStartDocument,
    "\n  mutation AuthLogout {\n    authLogout\n  }\n": types.AuthLogoutDocument,
    "\n  subscription ClustersWatch {\n    clustersWatch {\n      id\n      spec {\n        name\n        syncEnabled\n        enabled\n        source {\n          kubeconfig {\n            context\n          }\n        }\n      }\n      status {\n        source {\n          kubeconfig {\n            cluster\n            user\n            isPresent\n            isDefault\n          }\n        }\n        server {\n          uid\n        }\n        lastConnectedAt\n        conditions {\n          type\n          status\n          reason\n          message\n          lastTransitionTime\n        }\n      }\n      activeCache {\n        id\n        serverUid\n        enabled\n        status {\n          conditions {\n            type\n            status\n            reason\n          }\n          lastSyncedAt\n        }\n        stats {\n          exists\n          bytes\n        }\n      }\n      nextAttemptAt\n      connectionAttempts {\n        ok\n        reason\n        message\n        count\n        firstAt\n        lastAt\n      }\n    }\n  }\n": types.ClustersWatchDocument,
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
export function graphql(source: "\n  subscription ClustersWatch {\n    clustersWatch {\n      id\n      spec {\n        name\n        syncEnabled\n        enabled\n        source {\n          kubeconfig {\n            context\n          }\n        }\n      }\n      status {\n        source {\n          kubeconfig {\n            cluster\n            user\n            isPresent\n            isDefault\n          }\n        }\n        server {\n          uid\n        }\n        lastConnectedAt\n        conditions {\n          type\n          status\n          reason\n          message\n          lastTransitionTime\n        }\n      }\n      activeCache {\n        id\n        serverUid\n        enabled\n        status {\n          conditions {\n            type\n            status\n            reason\n          }\n          lastSyncedAt\n        }\n        stats {\n          exists\n          bytes\n        }\n      }\n      nextAttemptAt\n      connectionAttempts {\n        ok\n        reason\n        message\n        count\n        firstAt\n        lastAt\n      }\n    }\n  }\n"): (typeof documents)["\n  subscription ClustersWatch {\n    clustersWatch {\n      id\n      spec {\n        name\n        syncEnabled\n        enabled\n        source {\n          kubeconfig {\n            context\n          }\n        }\n      }\n      status {\n        source {\n          kubeconfig {\n            cluster\n            user\n            isPresent\n            isDefault\n          }\n        }\n        server {\n          uid\n        }\n        lastConnectedAt\n        conditions {\n          type\n          status\n          reason\n          message\n          lastTransitionTime\n        }\n      }\n      activeCache {\n        id\n        serverUid\n        enabled\n        status {\n          conditions {\n            type\n            status\n            reason\n          }\n          lastSyncedAt\n        }\n        stats {\n          exists\n          bytes\n        }\n      }\n      nextAttemptAt\n      connectionAttempts {\n        ok\n        reason\n        message\n        count\n        firstAt\n        lastAt\n      }\n    }\n  }\n"];
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  subscription ChatStream($input: ChatInput!) {\n    chatStream(input: $input) {\n      delta\n      done\n    }\n  }\n"): (typeof documents)["\n  subscription ChatStream($input: ChatInput!) {\n    chatStream(input: $input) {\n      delta\n      done\n    }\n  }\n"];

export function graphql(source: string) {
  return (documents as any)[source] ?? {};
}

export type DocumentType<TDocumentNode extends DocumentNode<any, any>> = TDocumentNode extends DocumentNode<  infer TType,  any>  ? TType  : never;