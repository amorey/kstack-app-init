/* eslint-disable */
/** Internal type. DO NOT USE DIRECTLY. */
type Exact<T extends { [key: string]: unknown }> = { [K in keyof T]: T[K] };
/** Internal type. DO NOT USE DIRECTLY. */
export type Incremental<T> = T | { [P in keyof T]?: P extends ' $fragmentName' | '__typename' ? T[P] : never };
import type { TypedDocumentNode as DocumentNode } from '@graphql-typed-document-node/core';
/**
 * Classifies a delta-watch change, mirroring a Kubernetes watch event. On subscribe a
 * watch replays the current set as `Added` changes (the snapshot), closes it with one
 * `Bookmark`, then streams live changes. A `Deleted` change carries the object's
 * last-known state; the client keys on its `id`.
 */
export type ChangeType =
  | 'Added'
  /**
   * Closes the on-subscribe snapshot: sent exactly once per stream, after the last
   * snapshot object and before the first live change. It carries **no object** — the
   * change's entity field is null, the only case in which it is — so skip it rather
   * than key on it. It is also what makes the collection fully known: a client must
   * not render an empty state before it arrives. Kubernetes' `initial-events-end`
   * bookmark, in this protocol's vocabulary.
   */
  | 'Bookmark'
  | 'Deleted'
  | 'Modified';

export type ChatInput = {
  messages: Array<ChatMessageInput>;
};

export type ChatMessageInput = {
  content: string;
  role: string;
};

/** A condition's three-valued verdict, Kubernetes-style. */
export type ConditionStatus =
  /** The condition does not hold. */
  | 'False'
  /** The condition holds. */
  | 'True'
  /** The condition cannot currently be assessed. */
  | 'Unknown';

/** An event's severity, mirroring the control plane's event type: Normal (✓) or Warning (✗). */
export type EventType =
  | 'Normal'
  | 'Warning';

export type ClusterEnabledSetMutationVariables = Exact<{
  id: string;
  enabled: boolean;
}>;


export type ClusterEnabledSetMutation = { clusterEnabledSet: { id: string, spec: { enabled: boolean } } };

export type ClusterSyncEnabledSetMutationVariables = Exact<{
  id: string;
  syncEnabled: boolean;
}>;


export type ClusterSyncEnabledSetMutation = { clusterSyncEnabledSet: { id: string, spec: { syncEnabled: boolean } } };

export type ClusterCacheClearMutationVariables = Exact<{
  id: string;
}>;


export type ClusterCacheClearMutation = { clusterCacheClear: { id: string } };

export type ClusterDeleteMutationVariables = Exact<{
  id: string;
}>;


export type ClusterDeleteMutation = { clusterDelete: boolean };

export type ClusterConnectionRetryMutationVariables = Exact<{
  id: string;
}>;


export type ClusterConnectionRetryMutation = { clusterConnectionRetry: boolean };

export type ClusterConnectionEventsSubscriptionVariables = Exact<{
  id: string;
}>;


export type ClusterConnectionEventsSubscription = { clusterEventsWatch: { id: string, type: EventType, reason: string, message: string, count: number, firstAt: string, lastAt: string } };

export type ClusterSyncEventsSubscriptionVariables = Exact<{
  id: string;
}>;


export type ClusterSyncEventsSubscription = { clusterCacheGVRSyncEventsWatch: { id: string, type: EventType, reason: string, message: string, count: number, firstAt: string, lastAt: string } };

export type ClusterCacheGvrDiscoveriesSubscriptionVariables = Exact<{ [key: string]: never; }>;


export type ClusterCacheGvrDiscoveriesSubscription = { clusterCacheGVRDiscoveriesWatch: { type: ChangeType, discovery: { id: string, cacheID: string, stats: { lastDiscoveryAt: string, resourceCount: number } | null, conditions: Array<{ type: string, reason: string, message: string, unconfirmed: boolean }> } | null } };

export type ClusterCacheStatsSubscriptionVariables = Exact<{
  id: string;
  cacheID: string;
}>;


export type ClusterCacheStatsSubscription = { clusterCacheStatsWatch: { exists: boolean, bytes: number, objectCount: number, kindCount: number } };

export type ClusterCacheGvrSyncsSubscriptionVariables = Exact<{
  cacheID: string;
}>;


export type ClusterCacheGvrSyncsSubscription = { clusterCacheGVRSyncsWatch: { type: ChangeType, sync: { id: string, spec: { apiVersion: string, resource: string } } | null } };

export type ClusterScheduleSubscriptionVariables = Exact<{
  id: string;
}>;


export type ClusterScheduleSubscription = { clusterScheduleWatch: { nextRequeueAt: string | null, probing: boolean } };

export type AuthStateWatchSubscriptionVariables = Exact<{ [key: string]: never; }>;


export type AuthStateWatchSubscription = { authStateWatch: { authenticated: boolean, identity: { sub: string, email: string, name: string } | null } };

export type AuthLoginStartMutationVariables = Exact<{ [key: string]: never; }>;


export type AuthLoginStartMutation = { authLoginStart: boolean };

export type AuthLogoutMutationVariables = Exact<{ [key: string]: never; }>;


export type AuthLogoutMutation = { authLogout: boolean };

export type ClusterDataEventsWatchSubscriptionVariables = Exact<{
  id: string;
  cacheID: string;
}>;


export type ClusterDataEventsWatchSubscription = { clusterDataEventsWatch: { type: ChangeType, cacheID: string, event: { uid: string, type: string, reason: string, message: string, count: number, firstSeen: string | null, lastSeen: string | null, involvedKind: string, involvedNamespace: string, involvedName: string } | null } };

export type ClusterDataKindsWatchSubscriptionVariables = Exact<{
  id: string;
  cacheID: string;
}>;


export type ClusterDataKindsWatchSubscription = { clusterDataKindsWatch: { type: ChangeType, cacheID: string, kind: { apiVersion: string, kind: string, resource: string, scope: string, isCRD: boolean, count: number } | null } };

export type ClusterDataObjectsWatchSubscriptionVariables = Exact<{
  id: string;
  cacheID: string;
  apiVersion: string;
  resource: string;
}>;


export type ClusterDataObjectsWatchSubscription = { clusterDataObjectsWatch: { type: ChangeType, cacheID: string, apiVersion: string, resource: string, object: { uid: string, apiVersion: string, kind: string, namespace: string, name: string, creationTimestamp: string | null, rawJSON: unknown } | null } };

export type ClustersWatchSubscriptionVariables = Exact<{ [key: string]: never; }>;


export type ClustersWatchSubscription = { clustersWatch: { type: ChangeType, cluster: { id: string, spec: { name: string | null, syncEnabled: boolean, enabled: boolean, source: { kubeconfig: { context: string } | null } }, status: { lastConnectedAt: string | null, source: { kubeconfig: { cluster: string, user: string, isPresent: boolean, isDefault: boolean } | null }, server: { uid: string | null } }, conditions: Array<{ type: string, status: ConditionStatus, reason: string, message: string, liveness: boolean, unconfirmed: boolean, transitionedAt: string }> } | null } };

export type ClusterCachesWatchSubscriptionVariables = Exact<{ [key: string]: never; }>;


export type ClusterCachesWatchSubscription = { clusterCachesWatch: { type: ChangeType, cache: { id: string, clusterID: string, serverUid: string } | null } };

export type ClusterCacheSyncHealthWatchSubscriptionVariables = Exact<{ [key: string]: never; }>;


export type ClusterCacheSyncHealthWatchSubscription = { clusterCacheSyncHealthWatch: { cacheID: string, status: ConditionStatus, reason: string, totalKinds: number, unhealthyKinds: number, lastUpdateAt: string | null, lastLiveAt: string | null, unhealthyKindRefs: Array<{ apiVersion: string, resource: string }> } };

export type ChatStreamSubscriptionVariables = Exact<{
  input: ChatInput;
}>;


export type ChatStreamSubscription = { chatStream: { delta: string, done: boolean } };


export const ClusterEnabledSetDocument = {"kind":"Document","definitions":[{"kind":"OperationDefinition","operation":"mutation","name":{"kind":"Name","value":"ClusterEnabledSet"},"variableDefinitions":[{"kind":"VariableDefinition","variable":{"kind":"Variable","name":{"kind":"Name","value":"id"}},"type":{"kind":"NonNullType","type":{"kind":"NamedType","name":{"kind":"Name","value":"ObjectID"}}}},{"kind":"VariableDefinition","variable":{"kind":"Variable","name":{"kind":"Name","value":"enabled"}},"type":{"kind":"NonNullType","type":{"kind":"NamedType","name":{"kind":"Name","value":"Boolean"}}}}],"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"clusterEnabledSet"},"arguments":[{"kind":"Argument","name":{"kind":"Name","value":"id"},"value":{"kind":"Variable","name":{"kind":"Name","value":"id"}}},{"kind":"Argument","name":{"kind":"Name","value":"enabled"},"value":{"kind":"Variable","name":{"kind":"Name","value":"enabled"}}}],"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"id"}},{"kind":"Field","name":{"kind":"Name","value":"spec"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"enabled"}}]}}]}}]}}]} as unknown as DocumentNode<ClusterEnabledSetMutation, ClusterEnabledSetMutationVariables>;
export const ClusterSyncEnabledSetDocument = {"kind":"Document","definitions":[{"kind":"OperationDefinition","operation":"mutation","name":{"kind":"Name","value":"ClusterSyncEnabledSet"},"variableDefinitions":[{"kind":"VariableDefinition","variable":{"kind":"Variable","name":{"kind":"Name","value":"id"}},"type":{"kind":"NonNullType","type":{"kind":"NamedType","name":{"kind":"Name","value":"ObjectID"}}}},{"kind":"VariableDefinition","variable":{"kind":"Variable","name":{"kind":"Name","value":"syncEnabled"}},"type":{"kind":"NonNullType","type":{"kind":"NamedType","name":{"kind":"Name","value":"Boolean"}}}}],"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"clusterSyncEnabledSet"},"arguments":[{"kind":"Argument","name":{"kind":"Name","value":"id"},"value":{"kind":"Variable","name":{"kind":"Name","value":"id"}}},{"kind":"Argument","name":{"kind":"Name","value":"syncEnabled"},"value":{"kind":"Variable","name":{"kind":"Name","value":"syncEnabled"}}}],"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"id"}},{"kind":"Field","name":{"kind":"Name","value":"spec"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"syncEnabled"}}]}}]}}]}}]} as unknown as DocumentNode<ClusterSyncEnabledSetMutation, ClusterSyncEnabledSetMutationVariables>;
export const ClusterCacheClearDocument = {"kind":"Document","definitions":[{"kind":"OperationDefinition","operation":"mutation","name":{"kind":"Name","value":"ClusterCacheClear"},"variableDefinitions":[{"kind":"VariableDefinition","variable":{"kind":"Variable","name":{"kind":"Name","value":"id"}},"type":{"kind":"NonNullType","type":{"kind":"NamedType","name":{"kind":"Name","value":"ObjectID"}}}}],"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"clusterCacheClear"},"arguments":[{"kind":"Argument","name":{"kind":"Name","value":"id"},"value":{"kind":"Variable","name":{"kind":"Name","value":"id"}}}],"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"id"}}]}}]}}]} as unknown as DocumentNode<ClusterCacheClearMutation, ClusterCacheClearMutationVariables>;
export const ClusterDeleteDocument = {"kind":"Document","definitions":[{"kind":"OperationDefinition","operation":"mutation","name":{"kind":"Name","value":"ClusterDelete"},"variableDefinitions":[{"kind":"VariableDefinition","variable":{"kind":"Variable","name":{"kind":"Name","value":"id"}},"type":{"kind":"NonNullType","type":{"kind":"NamedType","name":{"kind":"Name","value":"ObjectID"}}}}],"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"clusterDelete"},"arguments":[{"kind":"Argument","name":{"kind":"Name","value":"id"},"value":{"kind":"Variable","name":{"kind":"Name","value":"id"}}}]}]}}]} as unknown as DocumentNode<ClusterDeleteMutation, ClusterDeleteMutationVariables>;
export const ClusterConnectionRetryDocument = {"kind":"Document","definitions":[{"kind":"OperationDefinition","operation":"mutation","name":{"kind":"Name","value":"ClusterConnectionRetry"},"variableDefinitions":[{"kind":"VariableDefinition","variable":{"kind":"Variable","name":{"kind":"Name","value":"id"}},"type":{"kind":"NonNullType","type":{"kind":"NamedType","name":{"kind":"Name","value":"ObjectID"}}}}],"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"clusterConnectionRetry"},"arguments":[{"kind":"Argument","name":{"kind":"Name","value":"id"},"value":{"kind":"Variable","name":{"kind":"Name","value":"id"}}}]}]}}]} as unknown as DocumentNode<ClusterConnectionRetryMutation, ClusterConnectionRetryMutationVariables>;
export const ClusterConnectionEventsDocument = {"kind":"Document","definitions":[{"kind":"OperationDefinition","operation":"subscription","name":{"kind":"Name","value":"ClusterConnectionEvents"},"variableDefinitions":[{"kind":"VariableDefinition","variable":{"kind":"Variable","name":{"kind":"Name","value":"id"}},"type":{"kind":"NonNullType","type":{"kind":"NamedType","name":{"kind":"Name","value":"ObjectID"}}}}],"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"clusterEventsWatch"},"arguments":[{"kind":"Argument","name":{"kind":"Name","value":"id"},"value":{"kind":"Variable","name":{"kind":"Name","value":"id"}}},{"kind":"Argument","name":{"kind":"Name","value":"category"},"value":{"kind":"StringValue","value":"connection","block":false}}],"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"id"}},{"kind":"Field","name":{"kind":"Name","value":"type"}},{"kind":"Field","name":{"kind":"Name","value":"reason"}},{"kind":"Field","name":{"kind":"Name","value":"message"}},{"kind":"Field","name":{"kind":"Name","value":"count"}},{"kind":"Field","name":{"kind":"Name","value":"firstAt"}},{"kind":"Field","name":{"kind":"Name","value":"lastAt"}}]}}]}}]} as unknown as DocumentNode<ClusterConnectionEventsSubscription, ClusterConnectionEventsSubscriptionVariables>;
export const ClusterSyncEventsDocument = {"kind":"Document","definitions":[{"kind":"OperationDefinition","operation":"subscription","name":{"kind":"Name","value":"ClusterSyncEvents"},"variableDefinitions":[{"kind":"VariableDefinition","variable":{"kind":"Variable","name":{"kind":"Name","value":"id"}},"type":{"kind":"NonNullType","type":{"kind":"NamedType","name":{"kind":"Name","value":"ObjectID"}}}}],"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"clusterCacheGVRSyncEventsWatch"},"arguments":[{"kind":"Argument","name":{"kind":"Name","value":"id"},"value":{"kind":"Variable","name":{"kind":"Name","value":"id"}}},{"kind":"Argument","name":{"kind":"Name","value":"category"},"value":{"kind":"StringValue","value":"sync","block":false}}],"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"id"}},{"kind":"Field","name":{"kind":"Name","value":"type"}},{"kind":"Field","name":{"kind":"Name","value":"reason"}},{"kind":"Field","name":{"kind":"Name","value":"message"}},{"kind":"Field","name":{"kind":"Name","value":"count"}},{"kind":"Field","name":{"kind":"Name","value":"firstAt"}},{"kind":"Field","name":{"kind":"Name","value":"lastAt"}}]}}]}}]} as unknown as DocumentNode<ClusterSyncEventsSubscription, ClusterSyncEventsSubscriptionVariables>;
export const ClusterCacheGvrDiscoveriesDocument = {"kind":"Document","definitions":[{"kind":"OperationDefinition","operation":"subscription","name":{"kind":"Name","value":"ClusterCacheGVRDiscoveries"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"clusterCacheGVRDiscoveriesWatch"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"type"}},{"kind":"Field","name":{"kind":"Name","value":"discovery"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"id"}},{"kind":"Field","name":{"kind":"Name","value":"cacheID"}},{"kind":"Field","name":{"kind":"Name","value":"stats"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"lastDiscoveryAt"}},{"kind":"Field","name":{"kind":"Name","value":"resourceCount"}}]}},{"kind":"Field","name":{"kind":"Name","value":"conditions"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"type"}},{"kind":"Field","name":{"kind":"Name","value":"reason"}},{"kind":"Field","name":{"kind":"Name","value":"message"}},{"kind":"Field","name":{"kind":"Name","value":"unconfirmed"}}]}}]}}]}}]}}]} as unknown as DocumentNode<ClusterCacheGvrDiscoveriesSubscription, ClusterCacheGvrDiscoveriesSubscriptionVariables>;
export const ClusterCacheStatsDocument = {"kind":"Document","definitions":[{"kind":"OperationDefinition","operation":"subscription","name":{"kind":"Name","value":"ClusterCacheStats"},"variableDefinitions":[{"kind":"VariableDefinition","variable":{"kind":"Variable","name":{"kind":"Name","value":"id"}},"type":{"kind":"NonNullType","type":{"kind":"NamedType","name":{"kind":"Name","value":"ObjectID"}}}},{"kind":"VariableDefinition","variable":{"kind":"Variable","name":{"kind":"Name","value":"cacheID"}},"type":{"kind":"NonNullType","type":{"kind":"NamedType","name":{"kind":"Name","value":"ObjectID"}}}}],"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"clusterCacheStatsWatch"},"arguments":[{"kind":"Argument","name":{"kind":"Name","value":"id"},"value":{"kind":"Variable","name":{"kind":"Name","value":"id"}}},{"kind":"Argument","name":{"kind":"Name","value":"cacheID"},"value":{"kind":"Variable","name":{"kind":"Name","value":"cacheID"}}}],"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"exists"}},{"kind":"Field","name":{"kind":"Name","value":"bytes"}},{"kind":"Field","name":{"kind":"Name","value":"objectCount"}},{"kind":"Field","name":{"kind":"Name","value":"kindCount"}}]}}]}}]} as unknown as DocumentNode<ClusterCacheStatsSubscription, ClusterCacheStatsSubscriptionVariables>;
export const ClusterCacheGvrSyncsDocument = {"kind":"Document","definitions":[{"kind":"OperationDefinition","operation":"subscription","name":{"kind":"Name","value":"ClusterCacheGVRSyncs"},"variableDefinitions":[{"kind":"VariableDefinition","variable":{"kind":"Variable","name":{"kind":"Name","value":"cacheID"}},"type":{"kind":"NonNullType","type":{"kind":"NamedType","name":{"kind":"Name","value":"ObjectID"}}}}],"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"clusterCacheGVRSyncsWatch"},"arguments":[{"kind":"Argument","name":{"kind":"Name","value":"cacheID"},"value":{"kind":"Variable","name":{"kind":"Name","value":"cacheID"}}}],"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"type"}},{"kind":"Field","name":{"kind":"Name","value":"sync"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"id"}},{"kind":"Field","name":{"kind":"Name","value":"spec"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"apiVersion"}},{"kind":"Field","name":{"kind":"Name","value":"resource"}}]}}]}}]}}]}}]} as unknown as DocumentNode<ClusterCacheGvrSyncsSubscription, ClusterCacheGvrSyncsSubscriptionVariables>;
export const ClusterScheduleDocument = {"kind":"Document","definitions":[{"kind":"OperationDefinition","operation":"subscription","name":{"kind":"Name","value":"ClusterSchedule"},"variableDefinitions":[{"kind":"VariableDefinition","variable":{"kind":"Variable","name":{"kind":"Name","value":"id"}},"type":{"kind":"NonNullType","type":{"kind":"NamedType","name":{"kind":"Name","value":"ObjectID"}}}}],"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"clusterScheduleWatch"},"arguments":[{"kind":"Argument","name":{"kind":"Name","value":"id"},"value":{"kind":"Variable","name":{"kind":"Name","value":"id"}}}],"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"nextRequeueAt"}},{"kind":"Field","name":{"kind":"Name","value":"probing"}}]}}]}}]} as unknown as DocumentNode<ClusterScheduleSubscription, ClusterScheduleSubscriptionVariables>;
export const AuthStateWatchDocument = {"kind":"Document","definitions":[{"kind":"OperationDefinition","operation":"subscription","name":{"kind":"Name","value":"AuthStateWatch"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"authStateWatch"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"authenticated"}},{"kind":"Field","name":{"kind":"Name","value":"identity"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"sub"}},{"kind":"Field","name":{"kind":"Name","value":"email"}},{"kind":"Field","name":{"kind":"Name","value":"name"}}]}}]}}]}}]} as unknown as DocumentNode<AuthStateWatchSubscription, AuthStateWatchSubscriptionVariables>;
export const AuthLoginStartDocument = {"kind":"Document","definitions":[{"kind":"OperationDefinition","operation":"mutation","name":{"kind":"Name","value":"AuthLoginStart"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"authLoginStart"}}]}}]} as unknown as DocumentNode<AuthLoginStartMutation, AuthLoginStartMutationVariables>;
export const AuthLogoutDocument = {"kind":"Document","definitions":[{"kind":"OperationDefinition","operation":"mutation","name":{"kind":"Name","value":"AuthLogout"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"authLogout"}}]}}]} as unknown as DocumentNode<AuthLogoutMutation, AuthLogoutMutationVariables>;
export const ClusterDataEventsWatchDocument = {"kind":"Document","definitions":[{"kind":"OperationDefinition","operation":"subscription","name":{"kind":"Name","value":"ClusterDataEventsWatch"},"variableDefinitions":[{"kind":"VariableDefinition","variable":{"kind":"Variable","name":{"kind":"Name","value":"id"}},"type":{"kind":"NonNullType","type":{"kind":"NamedType","name":{"kind":"Name","value":"ObjectID"}}}},{"kind":"VariableDefinition","variable":{"kind":"Variable","name":{"kind":"Name","value":"cacheID"}},"type":{"kind":"NonNullType","type":{"kind":"NamedType","name":{"kind":"Name","value":"ObjectID"}}}}],"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"clusterDataEventsWatch"},"arguments":[{"kind":"Argument","name":{"kind":"Name","value":"id"},"value":{"kind":"Variable","name":{"kind":"Name","value":"id"}}},{"kind":"Argument","name":{"kind":"Name","value":"cacheID"},"value":{"kind":"Variable","name":{"kind":"Name","value":"cacheID"}}}],"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"type"}},{"kind":"Field","name":{"kind":"Name","value":"cacheID"}},{"kind":"Field","name":{"kind":"Name","value":"event"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"uid"}},{"kind":"Field","name":{"kind":"Name","value":"type"}},{"kind":"Field","name":{"kind":"Name","value":"reason"}},{"kind":"Field","name":{"kind":"Name","value":"message"}},{"kind":"Field","name":{"kind":"Name","value":"count"}},{"kind":"Field","name":{"kind":"Name","value":"firstSeen"}},{"kind":"Field","name":{"kind":"Name","value":"lastSeen"}},{"kind":"Field","name":{"kind":"Name","value":"involvedKind"}},{"kind":"Field","name":{"kind":"Name","value":"involvedNamespace"}},{"kind":"Field","name":{"kind":"Name","value":"involvedName"}}]}}]}}]}}]} as unknown as DocumentNode<ClusterDataEventsWatchSubscription, ClusterDataEventsWatchSubscriptionVariables>;
export const ClusterDataKindsWatchDocument = {"kind":"Document","definitions":[{"kind":"OperationDefinition","operation":"subscription","name":{"kind":"Name","value":"ClusterDataKindsWatch"},"variableDefinitions":[{"kind":"VariableDefinition","variable":{"kind":"Variable","name":{"kind":"Name","value":"id"}},"type":{"kind":"NonNullType","type":{"kind":"NamedType","name":{"kind":"Name","value":"ObjectID"}}}},{"kind":"VariableDefinition","variable":{"kind":"Variable","name":{"kind":"Name","value":"cacheID"}},"type":{"kind":"NonNullType","type":{"kind":"NamedType","name":{"kind":"Name","value":"ObjectID"}}}}],"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"clusterDataKindsWatch"},"arguments":[{"kind":"Argument","name":{"kind":"Name","value":"id"},"value":{"kind":"Variable","name":{"kind":"Name","value":"id"}}},{"kind":"Argument","name":{"kind":"Name","value":"cacheID"},"value":{"kind":"Variable","name":{"kind":"Name","value":"cacheID"}}}],"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"type"}},{"kind":"Field","name":{"kind":"Name","value":"cacheID"}},{"kind":"Field","name":{"kind":"Name","value":"kind"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"apiVersion"}},{"kind":"Field","name":{"kind":"Name","value":"kind"}},{"kind":"Field","name":{"kind":"Name","value":"resource"}},{"kind":"Field","name":{"kind":"Name","value":"scope"}},{"kind":"Field","name":{"kind":"Name","value":"isCRD"}},{"kind":"Field","name":{"kind":"Name","value":"count"}}]}}]}}]}}]} as unknown as DocumentNode<ClusterDataKindsWatchSubscription, ClusterDataKindsWatchSubscriptionVariables>;
export const ClusterDataObjectsWatchDocument = {"kind":"Document","definitions":[{"kind":"OperationDefinition","operation":"subscription","name":{"kind":"Name","value":"ClusterDataObjectsWatch"},"variableDefinitions":[{"kind":"VariableDefinition","variable":{"kind":"Variable","name":{"kind":"Name","value":"id"}},"type":{"kind":"NonNullType","type":{"kind":"NamedType","name":{"kind":"Name","value":"ObjectID"}}}},{"kind":"VariableDefinition","variable":{"kind":"Variable","name":{"kind":"Name","value":"cacheID"}},"type":{"kind":"NonNullType","type":{"kind":"NamedType","name":{"kind":"Name","value":"ObjectID"}}}},{"kind":"VariableDefinition","variable":{"kind":"Variable","name":{"kind":"Name","value":"apiVersion"}},"type":{"kind":"NonNullType","type":{"kind":"NamedType","name":{"kind":"Name","value":"String"}}}},{"kind":"VariableDefinition","variable":{"kind":"Variable","name":{"kind":"Name","value":"resource"}},"type":{"kind":"NonNullType","type":{"kind":"NamedType","name":{"kind":"Name","value":"String"}}}}],"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"clusterDataObjectsWatch"},"arguments":[{"kind":"Argument","name":{"kind":"Name","value":"id"},"value":{"kind":"Variable","name":{"kind":"Name","value":"id"}}},{"kind":"Argument","name":{"kind":"Name","value":"cacheID"},"value":{"kind":"Variable","name":{"kind":"Name","value":"cacheID"}}},{"kind":"Argument","name":{"kind":"Name","value":"apiVersion"},"value":{"kind":"Variable","name":{"kind":"Name","value":"apiVersion"}}},{"kind":"Argument","name":{"kind":"Name","value":"resource"},"value":{"kind":"Variable","name":{"kind":"Name","value":"resource"}}}],"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"type"}},{"kind":"Field","name":{"kind":"Name","value":"cacheID"}},{"kind":"Field","name":{"kind":"Name","value":"apiVersion"}},{"kind":"Field","name":{"kind":"Name","value":"resource"}},{"kind":"Field","name":{"kind":"Name","value":"object"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"uid"}},{"kind":"Field","name":{"kind":"Name","value":"apiVersion"}},{"kind":"Field","name":{"kind":"Name","value":"kind"}},{"kind":"Field","name":{"kind":"Name","value":"namespace"}},{"kind":"Field","name":{"kind":"Name","value":"name"}},{"kind":"Field","name":{"kind":"Name","value":"creationTimestamp"}},{"kind":"Field","name":{"kind":"Name","value":"rawJSON"}}]}}]}}]}}]} as unknown as DocumentNode<ClusterDataObjectsWatchSubscription, ClusterDataObjectsWatchSubscriptionVariables>;
export const ClustersWatchDocument = {"kind":"Document","definitions":[{"kind":"OperationDefinition","operation":"subscription","name":{"kind":"Name","value":"ClustersWatch"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"clustersWatch"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"type"}},{"kind":"Field","name":{"kind":"Name","value":"cluster"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"id"}},{"kind":"Field","name":{"kind":"Name","value":"spec"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"name"}},{"kind":"Field","name":{"kind":"Name","value":"syncEnabled"}},{"kind":"Field","name":{"kind":"Name","value":"enabled"}},{"kind":"Field","name":{"kind":"Name","value":"source"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"kubeconfig"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"context"}}]}}]}}]}},{"kind":"Field","name":{"kind":"Name","value":"status"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"source"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"kubeconfig"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"cluster"}},{"kind":"Field","name":{"kind":"Name","value":"user"}},{"kind":"Field","name":{"kind":"Name","value":"isPresent"}},{"kind":"Field","name":{"kind":"Name","value":"isDefault"}}]}}]}},{"kind":"Field","name":{"kind":"Name","value":"server"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"uid"}}]}},{"kind":"Field","name":{"kind":"Name","value":"lastConnectedAt"}}]}},{"kind":"Field","name":{"kind":"Name","value":"conditions"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"type"}},{"kind":"Field","name":{"kind":"Name","value":"status"}},{"kind":"Field","name":{"kind":"Name","value":"reason"}},{"kind":"Field","name":{"kind":"Name","value":"message"}},{"kind":"Field","name":{"kind":"Name","value":"liveness"}},{"kind":"Field","name":{"kind":"Name","value":"unconfirmed"}},{"kind":"Field","name":{"kind":"Name","value":"transitionedAt"}}]}}]}}]}}]}}]} as unknown as DocumentNode<ClustersWatchSubscription, ClustersWatchSubscriptionVariables>;
export const ClusterCachesWatchDocument = {"kind":"Document","definitions":[{"kind":"OperationDefinition","operation":"subscription","name":{"kind":"Name","value":"ClusterCachesWatch"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"clusterCachesWatch"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"type"}},{"kind":"Field","name":{"kind":"Name","value":"cache"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"id"}},{"kind":"Field","name":{"kind":"Name","value":"clusterID"}},{"kind":"Field","name":{"kind":"Name","value":"serverUid"}}]}}]}}]}}]} as unknown as DocumentNode<ClusterCachesWatchSubscription, ClusterCachesWatchSubscriptionVariables>;
export const ClusterCacheSyncHealthWatchDocument = {"kind":"Document","definitions":[{"kind":"OperationDefinition","operation":"subscription","name":{"kind":"Name","value":"ClusterCacheSyncHealthWatch"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"clusterCacheSyncHealthWatch"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"cacheID"}},{"kind":"Field","name":{"kind":"Name","value":"status"}},{"kind":"Field","name":{"kind":"Name","value":"reason"}},{"kind":"Field","name":{"kind":"Name","value":"unhealthyKindRefs"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"apiVersion"}},{"kind":"Field","name":{"kind":"Name","value":"resource"}}]}},{"kind":"Field","name":{"kind":"Name","value":"totalKinds"}},{"kind":"Field","name":{"kind":"Name","value":"unhealthyKinds"}},{"kind":"Field","name":{"kind":"Name","value":"lastUpdateAt"}},{"kind":"Field","name":{"kind":"Name","value":"lastLiveAt"}}]}}]}}]} as unknown as DocumentNode<ClusterCacheSyncHealthWatchSubscription, ClusterCacheSyncHealthWatchSubscriptionVariables>;
export const ChatStreamDocument = {"kind":"Document","definitions":[{"kind":"OperationDefinition","operation":"subscription","name":{"kind":"Name","value":"ChatStream"},"variableDefinitions":[{"kind":"VariableDefinition","variable":{"kind":"Variable","name":{"kind":"Name","value":"input"}},"type":{"kind":"NonNullType","type":{"kind":"NamedType","name":{"kind":"Name","value":"ChatInput"}}}}],"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"chatStream"},"arguments":[{"kind":"Argument","name":{"kind":"Name","value":"input"},"value":{"kind":"Variable","name":{"kind":"Name","value":"input"}}}],"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"delta"}},{"kind":"Field","name":{"kind":"Name","value":"done"}}]}}]}}]} as unknown as DocumentNode<ChatStreamSubscription, ChatStreamSubscriptionVariables>;