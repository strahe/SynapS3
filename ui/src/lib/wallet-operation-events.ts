import type { QueryClient } from '@tanstack/react-query'
import type { WalletOperation, WalletOperationsResponse } from '@/api/client'

interface WalletOperationEventPayload {
  topic?: string
  operation?: WalletOperation
}

export function applyWalletOperationEventData(queryClient: QueryClient, raw: string) {
  let payload: WalletOperationEventPayload
  try {
    payload = JSON.parse(raw) as WalletOperationEventPayload
  } catch {
    return
  }
  if (payload.topic !== 'wallet_operation_updated' || !payload.operation) return

  const operation = payload.operation
  for (const query of queryClient.getQueryCache().findAll({ queryKey: ['walletOperations'] })) {
    const limit = walletOperationsLimitFromKey(query.queryKey)
    queryClient.setQueryData<WalletOperationsResponse>(query.queryKey, (data) =>
      applyWalletOperationSnapshot(data, operation, limit)
    )
  }
  queryClient.invalidateQueries({ queryKey: ['walletOperations'] })
  queryClient.invalidateQueries({ queryKey: ['wallet'] })
}

function applyWalletOperationSnapshot(
  data: WalletOperationsResponse | undefined,
  operation: WalletOperation,
  limit: number | null
): WalletOperationsResponse {
  if (!data) return { operations: [operation] }

  const operations = data.operations.filter((item) => item.id !== operation.id)
  operations.push(operation)
  operations.sort((a, b) => Date.parse(b.created_at) - Date.parse(a.created_at) || b.id - a.id)
  return {
    ...data,
    operations: limit == null ? operations : operations.slice(0, limit),
  }
}

function walletOperationsLimitFromKey(queryKey: readonly unknown[]) {
  const raw = queryKey[1]
  return typeof raw === 'number' && Number.isFinite(raw) && raw > 0 ? raw : null
}
