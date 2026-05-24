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

/** Engine connection lifecycle. */
export type SyncState =
  | 'BACKOFF'
  | 'CONNECTING'
  | 'LIVE'
  | 'OFFLINE';

export type TickSubscriptionVariables = Exact<{ [key: string]: never; }>;


export type TickSubscription = { tick: number };

export type SyncStatusWatchSubscriptionVariables = Exact<{ [key: string]: never; }>;


export type SyncStatusWatchSubscription = { syncStatusWatch: { state: SyncState, lastError: string, lastSyncedAt: number, retryAt: number } };

export type ChatStreamSubscriptionVariables = Exact<{
  input: ChatInput;
}>;


export type ChatStreamSubscription = { chatStream: { delta: string, done: boolean } };


export const TickDocument = {"kind":"Document","definitions":[{"kind":"OperationDefinition","operation":"subscription","name":{"kind":"Name","value":"Tick"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"tick"}}]}}]} as unknown as DocumentNode<TickSubscription, TickSubscriptionVariables>;
export const SyncStatusWatchDocument = {"kind":"Document","definitions":[{"kind":"OperationDefinition","operation":"subscription","name":{"kind":"Name","value":"SyncStatusWatch"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"syncStatusWatch"},"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"state"}},{"kind":"Field","name":{"kind":"Name","value":"lastError"}},{"kind":"Field","name":{"kind":"Name","value":"lastSyncedAt"}},{"kind":"Field","name":{"kind":"Name","value":"retryAt"}}]}}]}}]} as unknown as DocumentNode<SyncStatusWatchSubscription, SyncStatusWatchSubscriptionVariables>;
export const ChatStreamDocument = {"kind":"Document","definitions":[{"kind":"OperationDefinition","operation":"subscription","name":{"kind":"Name","value":"ChatStream"},"variableDefinitions":[{"kind":"VariableDefinition","variable":{"kind":"Variable","name":{"kind":"Name","value":"input"}},"type":{"kind":"NonNullType","type":{"kind":"NamedType","name":{"kind":"Name","value":"ChatInput"}}}}],"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"chatStream"},"arguments":[{"kind":"Argument","name":{"kind":"Name","value":"input"},"value":{"kind":"Variable","name":{"kind":"Name","value":"input"}}}],"selectionSet":{"kind":"SelectionSet","selections":[{"kind":"Field","name":{"kind":"Name","value":"delta"}},{"kind":"Field","name":{"kind":"Name","value":"done"}}]}}]}}]} as unknown as DocumentNode<ChatStreamSubscription, ChatStreamSubscriptionVariables>;