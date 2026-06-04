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
    "\n  mutation SetClusterEnabled($uuid: String!, $enabled: Boolean!) {\n    setClusterEnabled(uuid: $uuid, enabled: $enabled) {\n      uuid\n      enabled\n    }\n  }\n": typeof types.SetClusterEnabledDocument,
    "\n  mutation DeleteClusterCache($uuid: String!) {\n    deleteClusterCache(uuid: $uuid)\n  }\n": typeof types.DeleteClusterCacheDocument,
    "\n  mutation RemoveCluster($uuid: String!) {\n    removeCluster(uuid: $uuid)\n  }\n": typeof types.RemoveClusterDocument,
    "\n  subscription AuthStateWatch {\n    authStateWatch {\n      authenticated\n      identity {\n        sub\n        email\n        name\n      }\n    }\n  }\n": typeof types.AuthStateWatchDocument,
    "\n  mutation StartLogin {\n    startLogin\n  }\n": typeof types.StartLoginDocument,
    "\n  mutation Logout {\n    logout\n  }\n": typeof types.LogoutDocument,
    "\n  subscription ClustersWatch {\n    clustersWatch {\n      uuid\n      name\n      context\n      isCurrent\n      enabled\n      present\n      cached\n      cacheBytes\n      lastSyncedAt\n      lastSeenInKubeconfigAt\n    }\n  }\n": typeof types.ClustersWatchDocument,
    "\n  subscription Tick {\n    tick\n  }\n": typeof types.TickDocument,
    "\n  subscription KubeConfigWatch {\n    kubeConfigWatch {\n      type\n      object {\n        currentContext\n        authInfos {\n          name\n          locationOfOrigin\n        }\n        clusters {\n          name\n          locationOfOrigin\n          server\n        }\n        contexts {\n          name\n          locationOfOrigin\n          cluster\n          authInfo\n          namespace\n        }\n      }\n    }\n  }\n": typeof types.KubeConfigWatchDocument,
    "\n  subscription ChatStream($input: ChatInput!) {\n    chatStream(input: $input) {\n      delta\n      done\n    }\n  }\n": typeof types.ChatStreamDocument,
};
const documents: Documents = {
    "\n  mutation SetClusterEnabled($uuid: String!, $enabled: Boolean!) {\n    setClusterEnabled(uuid: $uuid, enabled: $enabled) {\n      uuid\n      enabled\n    }\n  }\n": types.SetClusterEnabledDocument,
    "\n  mutation DeleteClusterCache($uuid: String!) {\n    deleteClusterCache(uuid: $uuid)\n  }\n": types.DeleteClusterCacheDocument,
    "\n  mutation RemoveCluster($uuid: String!) {\n    removeCluster(uuid: $uuid)\n  }\n": types.RemoveClusterDocument,
    "\n  subscription AuthStateWatch {\n    authStateWatch {\n      authenticated\n      identity {\n        sub\n        email\n        name\n      }\n    }\n  }\n": types.AuthStateWatchDocument,
    "\n  mutation StartLogin {\n    startLogin\n  }\n": types.StartLoginDocument,
    "\n  mutation Logout {\n    logout\n  }\n": types.LogoutDocument,
    "\n  subscription ClustersWatch {\n    clustersWatch {\n      uuid\n      name\n      context\n      isCurrent\n      enabled\n      present\n      cached\n      cacheBytes\n      lastSyncedAt\n      lastSeenInKubeconfigAt\n    }\n  }\n": types.ClustersWatchDocument,
    "\n  subscription Tick {\n    tick\n  }\n": types.TickDocument,
    "\n  subscription KubeConfigWatch {\n    kubeConfigWatch {\n      type\n      object {\n        currentContext\n        authInfos {\n          name\n          locationOfOrigin\n        }\n        clusters {\n          name\n          locationOfOrigin\n          server\n        }\n        contexts {\n          name\n          locationOfOrigin\n          cluster\n          authInfo\n          namespace\n        }\n      }\n    }\n  }\n": types.KubeConfigWatchDocument,
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
export function graphql(source: "\n  mutation SetClusterEnabled($uuid: String!, $enabled: Boolean!) {\n    setClusterEnabled(uuid: $uuid, enabled: $enabled) {\n      uuid\n      enabled\n    }\n  }\n"): (typeof documents)["\n  mutation SetClusterEnabled($uuid: String!, $enabled: Boolean!) {\n    setClusterEnabled(uuid: $uuid, enabled: $enabled) {\n      uuid\n      enabled\n    }\n  }\n"];
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  mutation DeleteClusterCache($uuid: String!) {\n    deleteClusterCache(uuid: $uuid)\n  }\n"): (typeof documents)["\n  mutation DeleteClusterCache($uuid: String!) {\n    deleteClusterCache(uuid: $uuid)\n  }\n"];
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  mutation RemoveCluster($uuid: String!) {\n    removeCluster(uuid: $uuid)\n  }\n"): (typeof documents)["\n  mutation RemoveCluster($uuid: String!) {\n    removeCluster(uuid: $uuid)\n  }\n"];
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  subscription AuthStateWatch {\n    authStateWatch {\n      authenticated\n      identity {\n        sub\n        email\n        name\n      }\n    }\n  }\n"): (typeof documents)["\n  subscription AuthStateWatch {\n    authStateWatch {\n      authenticated\n      identity {\n        sub\n        email\n        name\n      }\n    }\n  }\n"];
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  mutation StartLogin {\n    startLogin\n  }\n"): (typeof documents)["\n  mutation StartLogin {\n    startLogin\n  }\n"];
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  mutation Logout {\n    logout\n  }\n"): (typeof documents)["\n  mutation Logout {\n    logout\n  }\n"];
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  subscription ClustersWatch {\n    clustersWatch {\n      uuid\n      name\n      context\n      isCurrent\n      enabled\n      present\n      cached\n      cacheBytes\n      lastSyncedAt\n      lastSeenInKubeconfigAt\n    }\n  }\n"): (typeof documents)["\n  subscription ClustersWatch {\n    clustersWatch {\n      uuid\n      name\n      context\n      isCurrent\n      enabled\n      present\n      cached\n      cacheBytes\n      lastSyncedAt\n      lastSeenInKubeconfigAt\n    }\n  }\n"];
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  subscription Tick {\n    tick\n  }\n"): (typeof documents)["\n  subscription Tick {\n    tick\n  }\n"];
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  subscription KubeConfigWatch {\n    kubeConfigWatch {\n      type\n      object {\n        currentContext\n        authInfos {\n          name\n          locationOfOrigin\n        }\n        clusters {\n          name\n          locationOfOrigin\n          server\n        }\n        contexts {\n          name\n          locationOfOrigin\n          cluster\n          authInfo\n          namespace\n        }\n      }\n    }\n  }\n"): (typeof documents)["\n  subscription KubeConfigWatch {\n    kubeConfigWatch {\n      type\n      object {\n        currentContext\n        authInfos {\n          name\n          locationOfOrigin\n        }\n        clusters {\n          name\n          locationOfOrigin\n          server\n        }\n        contexts {\n          name\n          locationOfOrigin\n          cluster\n          authInfo\n          namespace\n        }\n      }\n    }\n  }\n"];
/**
 * The graphql function is used to parse GraphQL queries into a document that can be used by GraphQL clients.
 */
export function graphql(source: "\n  subscription ChatStream($input: ChatInput!) {\n    chatStream(input: $input) {\n      delta\n      done\n    }\n  }\n"): (typeof documents)["\n  subscription ChatStream($input: ChatInput!) {\n    chatStream(input: $input) {\n      delta\n      done\n    }\n  }\n"];

export function graphql(source: string) {
  return (documents as any)[source] ?? {};
}

export type DocumentType<TDocumentNode extends DocumentNode<any, any>> = TDocumentNode extends DocumentNode<  infer TType,  any>  ? TType  : never;