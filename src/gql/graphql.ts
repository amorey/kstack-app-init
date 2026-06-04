/* eslint-disable */
/** Internal type. DO NOT USE DIRECTLY. */
type Exact<T extends { [key: string]: unknown }> = { [K in keyof T]: T[K] };
/** Internal type. DO NOT USE DIRECTLY. */
export type Incremental<T> = T | { [P in keyof T]?: P extends ' $fragmentName' | '__typename' ? T[P] : never };
import type { TypedDocumentNode as DocumentNode } from '@graphql-typed-document-node/core';
export type ChatInput = {
  messages: Array<ChatMessageInput>;
};

export type ChatMessageInput = {
  content: string;
  role: string;
};

export type WatchEventType =
  | 'ADDED'
  | 'BOOKMARK'
  | 'DELETED'
  | 'ERROR'
  | 'MODIFIED';

export type SetClusterEnabledMutationVariables = Exact<{
  uuid: string;
  enabled: boolean;
}>;


export type SetClusterEnabledMutation = { setClusterEnabled: { uuid: string, enabled: boolean } };

export type DeleteClusterCacheMutationVariables = Exact<{
  uuid: string;
}>;


export type DeleteClusterCacheMutation = { deleteClusterCache: boolean };

export type RemoveClusterMutationVariables = Exact<{
  uuid: string;
}>;


export type RemoveClusterMutation = { removeCluster: boolean };

export type AuthStateWatchSubscriptionVariables = Exact<{ [key: string]: never; }>;


export type AuthStateWatchSubscription = { authStateWatch: { authenticated: boolean, identity: { sub: string, email: string, name: string } | null } };

export type StartLoginMutationVariables = Exact<{ [key: string]: never; }>;


export type StartLoginMutation = { startLogin: boolean };

export type LogoutMutationVariables = Exact<{ [key: string]: never; }>;


export type LogoutMutation = { logout: boolean };

export type ClustersWatchSubscriptionVariables = Exact<{ [key: string]: never; }>;


export type ClustersWatchSubscription = { clustersWatch: Array<{ uuid: string, name: string, context: string, isCurrent: boolean, enabled: boolean, present: boolean, cached: boolean, cacheBytes: number, lastSyncedAt: number, lastSeenInKubeconfigAt: number }> };

export type TickSubscriptionVariables = Exact<{ [key: string]: never; }>;


export type TickSubscription = { tick: number };

export type KubeConfigWatchSubscriptionVariables = Exact<{ [key: string]: never; }>;


export type KubeConfigWatchSubscription = { kubeConfigWatch: { type: WatchEventType, object: { currentContext: string, authInfos: Array<{ name: string, locationOfOrigin: string }>, clusters: Array<{ name: string, locationOfOrigin: string, server: string }>, contexts: Array<{ name: string, locationOfOrigin: string, cluster: string, authInfo: string, namespace: string }> } | null } | null };

export type ChatStreamSubscriptionVariables = Exact<{
  input: ChatInput;
}>;


export type ChatStreamSubscription = { chatStream: { delta: string, done: boolean } };


export const SetClusterEnabledDocument = {"kind":"Document","definitions":[{"kind":"OperationDefinition","operation":"mutation","name":{"kind":"Name","value":"SetClusterEnabled"},"variableDefinitions":[{"kind":"VariableDefinition","variable":{"kind":"Variable","name":{"kind":"Name","value":"uuid"}},"type":{"kind":"NonNullType","type":{"kind":"NamedType","name":{"kind":"Name","value":"String"}}}},{"kind":"VariableDefinition","variable":{"kind":"Variable","name":{"kind":"Name","value":"enabled"}},"type":{"kind":"NonNullType","type":{"kind":"NamedType","name":{"kind":"Name","value":"Boolean"}}}}],"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"setClusterEnabled"},"arguments":[{"kind":"Argument","name":{"kind":"Name","value":"uuid"},"value":{"kind":"Variable","name":{"kind":"Name","value":"uuid"}}},{"kind":"Argument","name":{"kind":"Name","value":"enabled"},"value":{"kind":"Variable","name":{"kind":"Name","value":"enabled"}}}],"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"uuid"}},{"kind":"Field","name":{"kind":"Name","value":"enabled"}}]}}]}}]} as unknown as DocumentNode<SetClusterEnabledMutation, SetClusterEnabledMutationVariables>;
export const DeleteClusterCacheDocument = {"kind":"Document","definitions":[{"kind":"OperationDefinition","operation":"mutation","name":{"kind":"Name","value":"DeleteClusterCache"},"variableDefinitions":[{"kind":"VariableDefinition","variable":{"kind":"Variable","name":{"kind":"Name","value":"uuid"}},"type":{"kind":"NonNullType","type":{"kind":"NamedType","name":{"kind":"Name","value":"String"}}}}],"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"deleteClusterCache"},"arguments":[{"kind":"Argument","name":{"kind":"Name","value":"uuid"},"value":{"kind":"Variable","name":{"kind":"Name","value":"uuid"}}}]}]}}]} as unknown as DocumentNode<DeleteClusterCacheMutation, DeleteClusterCacheMutationVariables>;
export const RemoveClusterDocument = {"kind":"Document","definitions":[{"kind":"OperationDefinition","operation":"mutation","name":{"kind":"Name","value":"RemoveCluster"},"variableDefinitions":[{"kind":"VariableDefinition","variable":{"kind":"Variable","name":{"kind":"Name","value":"uuid"}},"type":{"kind":"NonNullType","type":{"kind":"NamedType","name":{"kind":"Name","value":"String"}}}}],"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"removeCluster"},"arguments":[{"kind":"Argument","name":{"kind":"Name","value":"uuid"},"value":{"kind":"Variable","name":{"kind":"Name","value":"uuid"}}}]}]}}]} as unknown as DocumentNode<RemoveClusterMutation, RemoveClusterMutationVariables>;
export const AuthStateWatchDocument = {"kind":"Document","definitions":[{"kind":"OperationDefinition","operation":"subscription","name":{"kind":"Name","value":"AuthStateWatch"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"authStateWatch"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"authenticated"}},{"kind":"Field","name":{"kind":"Name","value":"identity"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"sub"}},{"kind":"Field","name":{"kind":"Name","value":"email"}},{"kind":"Field","name":{"kind":"Name","value":"name"}}]}}]}}]}}]} as unknown as DocumentNode<AuthStateWatchSubscription, AuthStateWatchSubscriptionVariables>;
export const StartLoginDocument = {"kind":"Document","definitions":[{"kind":"OperationDefinition","operation":"mutation","name":{"kind":"Name","value":"StartLogin"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"startLogin"}}]}}]} as unknown as DocumentNode<StartLoginMutation, StartLoginMutationVariables>;
export const LogoutDocument = {"kind":"Document","definitions":[{"kind":"OperationDefinition","operation":"mutation","name":{"kind":"Name","value":"Logout"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"logout"}}]}}]} as unknown as DocumentNode<LogoutMutation, LogoutMutationVariables>;
export const ClustersWatchDocument = {"kind":"Document","definitions":[{"kind":"OperationDefinition","operation":"subscription","name":{"kind":"Name","value":"ClustersWatch"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"clustersWatch"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"uuid"}},{"kind":"Field","name":{"kind":"Name","value":"name"}},{"kind":"Field","name":{"kind":"Name","value":"context"}},{"kind":"Field","name":{"kind":"Name","value":"isCurrent"}},{"kind":"Field","name":{"kind":"Name","value":"enabled"}},{"kind":"Field","name":{"kind":"Name","value":"present"}},{"kind":"Field","name":{"kind":"Name","value":"cached"}},{"kind":"Field","name":{"kind":"Name","value":"cacheBytes"}},{"kind":"Field","name":{"kind":"Name","value":"lastSyncedAt"}},{"kind":"Field","name":{"kind":"Name","value":"lastSeenInKubeconfigAt"}}]}}]}}]} as unknown as DocumentNode<ClustersWatchSubscription, ClustersWatchSubscriptionVariables>;
export const TickDocument = {"kind":"Document","definitions":[{"kind":"OperationDefinition","operation":"subscription","name":{"kind":"Name","value":"Tick"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"tick"}}]}}]} as unknown as DocumentNode<TickSubscription, TickSubscriptionVariables>;
export const KubeConfigWatchDocument = {"kind":"Document","definitions":[{"kind":"OperationDefinition","operation":"subscription","name":{"kind":"Name","value":"KubeConfigWatch"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"kubeConfigWatch"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"type"}},{"kind":"Field","name":{"kind":"Name","value":"object"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"currentContext"}},{"kind":"Field","name":{"kind":"Name","value":"authInfos"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"name"}},{"kind":"Field","name":{"kind":"Name","value":"locationOfOrigin"}}]}},{"kind":"Field","name":{"kind":"Name","value":"clusters"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"name"}},{"kind":"Field","name":{"kind":"Name","value":"locationOfOrigin"}},{"kind":"Field","name":{"kind":"Name","value":"server"}}]}},{"kind":"Field","name":{"kind":"Name","value":"contexts"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"name"}},{"kind":"Field","name":{"kind":"Name","value":"locationOfOrigin"}},{"kind":"Field","name":{"kind":"Name","value":"cluster"}},{"kind":"Field","name":{"kind":"Name","value":"authInfo"}},{"kind":"Field","name":{"kind":"Name","value":"namespace"}}]}}]}}]}}]}}]} as unknown as DocumentNode<KubeConfigWatchSubscription, KubeConfigWatchSubscriptionVariables>;
export const ChatStreamDocument = {"kind":"Document","definitions":[{"kind":"OperationDefinition","operation":"subscription","name":{"kind":"Name","value":"ChatStream"},"variableDefinitions":[{"kind":"VariableDefinition","variable":{"kind":"Variable","name":{"kind":"Name","value":"input"}},"type":{"kind":"NonNullType","type":{"kind":"NamedType","name":{"kind":"Name","value":"ChatInput"}}}}],"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"chatStream"},"arguments":[{"kind":"Argument","name":{"kind":"Name","value":"input"},"value":{"kind":"Variable","name":{"kind":"Name","value":"input"}}}],"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"delta"}},{"kind":"Field","name":{"kind":"Name","value":"done"}}]}}]}}]} as unknown as DocumentNode<ChatStreamSubscription, ChatStreamSubscriptionVariables>;