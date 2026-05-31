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

/** Per-cluster sync lifecycle for the local-cache sync engine. */
export type ClusterSyncState =
  /** Retrying after an error. */
  | 'BACKOFF'
  /** Caught up; watching for incremental changes. */
  | 'LIVE'
  /** Cluster unreachable. */
  | 'OFFLINE'
  /** Queued; sync not started yet. */
  | 'PENDING'
  /** Actively downloading the cluster's resources into the local cache. */
  | 'SYNCING';

/** Engine connection lifecycle. */
export type SyncState =
  | 'BACKOFF'
  | 'CONNECTING'
  | 'LIVE'
  | 'OFFLINE';

export type WatchEventType =
  | 'ADDED'
  | 'BOOKMARK'
  | 'DELETED'
  | 'ERROR'
  | 'MODIFIED';

export type ClusterSyncStatusWatchSubscriptionVariables = Exact<{ [key: string]: never; }>;


export type ClusterSyncStatusWatchSubscription = { clusterSyncStatusWatch: Array<{ context: string, state: ClusterSyncState, lastError: string, lastSyncedAt: number, downloadRateBps: number }> };

export type TickSubscriptionVariables = Exact<{ [key: string]: never; }>;


export type TickSubscription = { tick: number };

export type KubeConfigWatchSubscriptionVariables = Exact<{ [key: string]: never; }>;


export type KubeConfigWatchSubscription = { kubeConfigWatch: { type: WatchEventType, object: { currentContext: string, authInfos: Array<{ name: string, locationOfOrigin: string }>, clusters: Array<{ name: string, locationOfOrigin: string, server: string }>, contexts: Array<{ name: string, locationOfOrigin: string, cluster: string, authInfo: string, namespace: string }> } | null } | null };

export type SyncStatusWatchSubscriptionVariables = Exact<{ [key: string]: never; }>;


export type SyncStatusWatchSubscription = { syncStatusWatch: { state: SyncState, lastError: string, lastSyncedAt: number, retryAt: number } };

export type ChatStreamSubscriptionVariables = Exact<{
  input: ChatInput;
}>;


export type ChatStreamSubscription = { chatStream: { delta: string, done: boolean } };


export const ClusterSyncStatusWatchDocument = {"kind":"Document","definitions":[{"kind":"OperationDefinition","operation":"subscription","name":{"kind":"Name","value":"ClusterSyncStatusWatch"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"clusterSyncStatusWatch"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"context"}},{"kind":"Field","name":{"kind":"Name","value":"state"}},{"kind":"Field","name":{"kind":"Name","value":"lastError"}},{"kind":"Field","name":{"kind":"Name","value":"lastSyncedAt"}},{"kind":"Field","name":{"kind":"Name","value":"downloadRateBps"}}]}}]}}]} as unknown as DocumentNode<ClusterSyncStatusWatchSubscription, ClusterSyncStatusWatchSubscriptionVariables>;
export const TickDocument = {"kind":"Document","definitions":[{"kind":"OperationDefinition","operation":"subscription","name":{"kind":"Name","value":"Tick"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"tick"}}]}}]} as unknown as DocumentNode<TickSubscription, TickSubscriptionVariables>;
export const KubeConfigWatchDocument = {"kind":"Document","definitions":[{"kind":"OperationDefinition","operation":"subscription","name":{"kind":"Name","value":"KubeConfigWatch"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"kubeConfigWatch"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"type"}},{"kind":"Field","name":{"kind":"Name","value":"object"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"currentContext"}},{"kind":"Field","name":{"kind":"Name","value":"authInfos"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"name"}},{"kind":"Field","name":{"kind":"Name","value":"locationOfOrigin"}}]}},{"kind":"Field","name":{"kind":"Name","value":"clusters"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"name"}},{"kind":"Field","name":{"kind":"Name","value":"locationOfOrigin"}},{"kind":"Field","name":{"kind":"Name","value":"server"}}]}},{"kind":"Field","name":{"kind":"Name","value":"contexts"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"name"}},{"kind":"Field","name":{"kind":"Name","value":"locationOfOrigin"}},{"kind":"Field","name":{"kind":"Name","value":"cluster"}},{"kind":"Field","name":{"kind":"Name","value":"authInfo"}},{"kind":"Field","name":{"kind":"Name","value":"namespace"}}]}}]}}]}}]}}]} as unknown as DocumentNode<KubeConfigWatchSubscription, KubeConfigWatchSubscriptionVariables>;
export const SyncStatusWatchDocument = {"kind":"Document","definitions":[{"kind":"OperationDefinition","operation":"subscription","name":{"kind":"Name","value":"SyncStatusWatch"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"syncStatusWatch"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"state"}},{"kind":"Field","name":{"kind":"Name","value":"lastError"}},{"kind":"Field","name":{"kind":"Name","value":"lastSyncedAt"}},{"kind":"Field","name":{"kind":"Name","value":"retryAt"}}]}}]}}]} as unknown as DocumentNode<SyncStatusWatchSubscription, SyncStatusWatchSubscriptionVariables>;
export const ChatStreamDocument = {"kind":"Document","definitions":[{"kind":"OperationDefinition","operation":"subscription","name":{"kind":"Name","value":"ChatStream"},"variableDefinitions":[{"kind":"VariableDefinition","variable":{"kind":"Variable","name":{"kind":"Name","value":"input"}},"type":{"kind":"NonNullType","type":{"kind":"NamedType","name":{"kind":"Name","value":"ChatInput"}}}}],"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"chatStream"},"arguments":[{"kind":"Argument","name":{"kind":"Name","value":"input"},"value":{"kind":"Variable","name":{"kind":"Name","value":"input"}}}],"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"delta"}},{"kind":"Field","name":{"kind":"Name","value":"done"}}]}}]}}]} as unknown as DocumentNode<ChatStreamSubscription, ChatStreamSubscriptionVariables>;