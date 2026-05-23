import type { TaskDiagnostic, TaskDiagnosticLiveState } from '@/api/client'
import type { StatusTone } from '@/components/app/StatusBadge'

export interface TaskDiagnosticFactRow {
  label: string
  value: string
  monospace?: boolean
  detail?: boolean
  detailValue?: string
  displayMaxLength?: number
}

export interface TaskDiagnosticViewModel {
  title: string
  primaryFacts: TaskDiagnosticFactRow[]
  detailFacts: TaskDiagnosticFactRow[]
}

export const taskDiagnosticSheetContentClassName =
  'data-[side=right]:w-[min(36rem,calc(100vw-2rem))] data-[side=right]:max-w-[calc(100vw-2rem)] data-[side=right]:sm:max-w-xl'

const stateLabels: Record<string, string> = {
  not_applicable: 'Not applicable',
  preparing: 'Preparing',
  transferring: 'Transferring',
  waiting_for_chain: 'Waiting for confirmation',
  confirmed: 'Confirmed',
  rejected: 'Rejected',
  mismatch: 'Mismatch',
  unavailable: 'Unavailable',
  unknown: 'Unknown',
}

const reasonLabels: Record<string, string> = {
  task_chain_pending: 'storage confirmation pending',
  task_chain_confirmed: 'storage confirmation recorded',
  task_transaction_rejected: 'provider transaction rejected',
  task_piece_status_mismatch: 'piece confirmation mismatch',
  task_diagnostic_unavailable: 'diagnostic evidence unavailable',
  task_rpc_unavailable: 'provider or RPC unavailable',
  task_insufficient_funds: 'payment wallet funds missing',
  task_missing_approval: 'wallet approval missing',
  task_missing_evidence: 'task evidence missing',
  task_unknown_status: 'task status unknown',
  task_not_applicable: 'task type not applicable',
  copy_pending: 'copy still pending',
}

export function buildTaskDiagnosticViewModel(diagnostic: TaskDiagnostic): TaskDiagnosticViewModel {
  return {
    title: diagnosticTitle(diagnostic),
    primaryFacts: primaryFacts(diagnostic),
    detailFacts: detailFacts(diagnostic),
  }
}

export function taskDiagnosticStateLabel(state: string) {
  return stateLabels[state] ?? humanize(state)
}

export function taskDiagnosticStateTone(state: string): StatusTone {
  switch (state) {
    case 'confirmed':
    case 'not_applicable':
      return 'success'
    case 'rejected':
    case 'mismatch':
    case 'unavailable':
      return 'danger'
    case 'waiting_for_chain':
    case 'preparing':
    case 'transferring':
      return 'info'
    case 'unknown':
      return 'warning'
    default:
      return 'neutral'
  }
}

export function taskDiagnosticReasonLabel(reason: string) {
  return reasonLabels[reason] ?? reason.replace(/^task_/, '').replace(/_/g, ' ')
}

export function shouldRefreshTaskDiagnostic(diagnostic: TaskDiagnostic) {
  const transaction = diagnostic.evidence.transaction
  switch (diagnostic.evidence.operation) {
    case 'create_data_set':
      return transaction?.kind === 'create_data_set' && Boolean(transaction.status_url)
    case 'add_pieces':
      return (
        transaction?.kind === 'add_pieces' &&
        Boolean(
          transaction.status_url || (transaction.service_url && transaction.data_set_id && transaction.transaction_id)
        )
      )
    default:
      return false
  }
}

function diagnosticTitle(diagnostic: TaskDiagnostic) {
  const operation = diagnostic.evidence.operation
  switch (diagnostic.current_state) {
    case 'not_applicable':
      return 'No upload diagnosis available'
    case 'preparing':
      return 'Preparing upload work'
    case 'transferring':
      return 'Sending data to provider'
    case 'waiting_for_chain':
      if (operation === 'create_data_set') return 'Waiting for data set confirmation'
      if (operation === 'add_pieces') return 'Waiting for storage confirmation'
      return 'Waiting for provider confirmation'
    case 'confirmed':
      if (operation === 'create_data_set') return 'Data set confirmation received'
      if (operation === 'add_pieces') return 'Storage confirmation received'
      return 'Task evidence is confirmed'
    case 'rejected':
      return 'Storage confirmation was rejected'
    case 'mismatch':
      return 'Provider confirmation does not match the submitted work'
    case 'unavailable':
      return 'Storage status check is unavailable'
    case 'unknown':
      return 'Diagnosis needs manual inspection'
    default:
      return taskDiagnosticStateLabel(diagnostic.current_state)
  }
}

function primaryFacts(diagnostic: TaskDiagnostic): TaskDiagnosticFactRow[] {
  const { evidence } = diagnostic
  const rows: TaskDiagnosticFactRow[] = []
  const providerID = evidence.copy?.provider_id ?? evidence.provider?.provider_id ?? evidence.data_set?.provider_id
  const providerStatus = evidence.provider?.status

  if (providerID || providerStatus) {
    rows.push({
      label: 'Provider',
      value: providerPrimaryValue(providerID, providerStatus),
    })
  }

  const dataSetValue = storageTargetValue(diagnostic)
  if (dataSetValue) rows.push({ label: 'Storage target', value: dataSetValue })

  const submittedWorkValue = submittedWork(diagnostic)
  if (submittedWorkValue) rows.push({ label: 'Pieces', value: submittedWorkValue })

  const confirmationValue = confirmationFact(diagnostic)
  if (confirmationValue) rows.push({ label: 'Confirmation', value: confirmationValue })

  const dataSetTx = evidence.data_set?.create_transaction_id
  if (dataSetTx && evidence.operation === 'create_data_set') {
    rows.push(transactionFact('Data set setup transaction', dataSetTx))
  }

  const addPiecesTx = evidence.copy?.commit_transaction_id
  if (addPiecesTx) {
    rows.push(transactionFact('Storage update transaction', addPiecesTx))
  }

  addPrimaryIssue(rows, 'Upload issue', evidence.upload?.error_message || evidence.upload?.accept_error)
  addPrimaryIssue(rows, 'Copy issue', evidence.copy?.last_error)
  addPrimaryIssue(rows, 'Data set issue', evidence.data_set?.last_error)
  addPrimaryIssue(rows, 'Provider issue', evidence.provider?.last_error)
  addPrimaryIssue(rows, 'Task issue', evidence.task.last_error || evidence.task.status_message)

  return rows
}

function detailFacts(diagnostic: TaskDiagnostic): TaskDiagnosticFactRow[] {
  const { evidence } = diagnostic
  const rows: TaskDiagnosticFactRow[] = []

  addFact(rows, 'Task ID', formatID(evidence.task.id))
  addFact(rows, 'Task type', evidence.task.type)
  addFact(rows, 'Task stage', evidence.task.stage)
  addFact(rows, 'Task status', evidence.task.status)
  addFact(rows, 'Retries', retryLabel(evidence.task.retry_count, evidence.task.max_retries))
  addFact(rows, 'Wait reason', evidence.task.wait_reason)
  addFact(rows, 'Operation', humanize(evidence.operation))

  if (evidence.upload) {
    addFact(rows, 'Upload ID', formatID(evidence.upload.id))
    addFact(rows, 'Upload status', evidence.upload.status)
    addFact(rows, 'Requested copies', numberValue(evidence.upload.requested_copies))
    addFact(rows, 'Upload error', evidence.upload.error_message || evidence.upload.accept_error, { detail: true })
  }

  if (evidence.copy) {
    addFact(rows, 'Copy replica', replicaLabel(evidence.copy.copy_index))
    addFact(rows, 'Copy status', evidence.copy.status)
    addFact(rows, 'Copy provider ID', evidence.copy.provider_id, { monospace: true })
    addFact(rows, 'Storage data set record', formatID(evidence.copy.storage_data_set_id))
    addFact(rows, 'Chain data set ID', evidence.copy.chain_data_set_id, { monospace: true })
    addFact(rows, 'Piece ID', evidence.copy.piece_id, { monospace: true })
    addFact(rows, 'Transfer method', evidence.copy.transfer_method)
    addFact(rows, 'Add-pieces transaction', evidence.copy.commit_transaction_id, { monospace: true, detail: true })
    addFact(rows, 'Copy error', evidence.copy.last_error, { detail: true })
  }

  if (evidence.data_set) {
    addFact(rows, 'Data set record', formatID(evidence.data_set.id))
    addFact(rows, 'Data set status', evidence.data_set.status)
    addFact(rows, 'Data set provider ID', evidence.data_set.provider_id, { monospace: true })
    addFact(rows, 'Data set copy', replicaLabel(evidence.data_set.copy_index))
    addFact(rows, 'Data set chain ID', evidence.data_set.chain_data_set_id, { monospace: true })
    addFact(rows, 'Client data set ID', evidence.data_set.client_data_set_id, {
      monospace: true,
      detail: true,
      displayMaxLength: 18,
    })
    addFact(rows, 'Data-set creation transaction', evidence.data_set.create_transaction_id, {
      monospace: true,
      detail: true,
    })
    addFact(rows, 'Data-set status URL', evidence.data_set.create_status_url, { monospace: true, detail: true })
    addFact(rows, 'Data set error', evidence.data_set.last_error, { detail: true })
  }

  if (evidence.provider) {
    addFact(rows, 'Provider ID', evidence.provider.provider_id, { monospace: true })
    addFact(rows, 'Provider status', evidence.provider.status)
    addFact(rows, 'Provider health status', evidence.provider.health_status)
    addFact(rows, 'Provider service URL', evidence.provider.service_url, { monospace: true, detail: true })
    addFact(rows, 'Provider reason codes', evidence.provider.reason_codes?.map(taskDiagnosticReasonLabel).join(', '))
    addFact(rows, 'Provider error', evidence.provider.last_error, { detail: true })
  }

  if (evidence.transaction) {
    addFact(rows, 'Transaction kind', humanize(evidence.transaction.kind))
    addFact(rows, 'Transaction status URL', evidence.transaction.status_url, { monospace: true, detail: true })
    addFact(rows, 'Transaction service URL', evidence.transaction.service_url, { monospace: true, detail: true })
    addFact(rows, 'Transaction data set ID', evidence.transaction.data_set_id, { monospace: true })
    addFact(rows, 'Transaction ID', evidence.transaction.transaction_id, { monospace: true, detail: true })
    addFact(rows, 'Transaction piece count', numberValue(evidence.transaction.piece_count))
  }

  if (evidence.live_check) {
    addFact(rows, 'Latest status check', evidence.live_check.state)
    addFact(rows, 'Recorded transaction status', evidence.live_check.tx_status)
    addFact(rows, 'Status check data set ID', evidence.live_check.data_set_id, { monospace: true })
    addFact(rows, 'Data set created', booleanStatus(evidence.live_check.data_set_created, evidence.live_check.state))
    addFact(
      rows,
      'Provider reported pieces added',
      booleanStatus(evidence.live_check.pieces_added, evidence.live_check.state)
    )
    addFact(rows, 'Piece count', recordedNumberValue(evidence.live_check.piece_count))
    addFact(rows, 'Confirmed piece IDs', listValue(evidence.live_check.confirmed_piece_ids), {
      monospace: true,
      detail: true,
    })
    addFact(rows, 'Status check error', evidence.live_check.error, { detail: true })
  }

  addFact(rows, 'Recorded reasons', diagnostic.reason_codes.map(taskDiagnosticReasonLabel).join(', '))
  addFact(rows, 'Diagnostic checked at', diagnostic.checked_at)

  return rows
}

function providerPrimaryValue(providerID?: string, status?: string) {
  const label = providerID ? `Provider #${providerID}` : 'Provider'
  switch (status) {
    case 'available':
      return `${label} is reachable`
    case 'degraded':
      return `${label} is degraded`
    case 'unavailable':
      return `${label} is unavailable`
    case 'unknown':
      return `${label} status is unknown`
    default:
      return `${label} selected`
  }
}

function storageTargetValue(diagnostic: TaskDiagnostic) {
  const { evidence } = diagnostic
  const chainDataSetID = evidence.copy?.chain_data_set_id ?? evidence.data_set?.chain_data_set_id
  const dataSetStatus = evidence.data_set?.status
  if (chainDataSetID) {
    return dataSetStatus
      ? `Data set #${chainDataSetID} is ${humanize(dataSetStatus)}`
      : `Data set #${chainDataSetID} selected`
  }
  if (evidence.data_set?.id !== undefined) {
    return dataSetStatus
      ? `Data set record #${evidence.data_set.id} is ${humanize(dataSetStatus)}`
      : `Data set record #${evidence.data_set.id} selected`
  }
  return ''
}

function submittedWork(diagnostic: TaskDiagnostic) {
  const { evidence } = diagnostic
  const pieceCount = evidence.transaction?.piece_count
  if (evidence.operation === 'add_pieces') {
    if (pieceCount !== undefined) return `${pieceCount} ${plural(pieceCount, 'piece')} submitted`
    if (evidence.copy?.piece_id) return '1 piece submitted'
    return 'Piece submission recorded'
  }
  if (evidence.operation === 'create_data_set') return 'Data set creation submitted'
  if (evidence.operation === 'transfer_piece') return 'Piece transfer in progress'
  if (evidence.operation === 'prepare_upload') return 'Upload work is being prepared'
  return ''
}

function confirmationFact(diagnostic: TaskDiagnostic) {
  const live = diagnostic.evidence.live_check
  const work = diagnostic.evidence.operation === 'create_data_set' ? 'data set setup' : 'storage update'
  if (live?.state) return liveStateConfirmation(live.state, work)

  switch (diagnostic.current_state) {
    case 'waiting_for_chain':
      return `Provider has not confirmed this ${work} yet`
    case 'confirmed':
      return `Provider confirmed this ${work}`
    case 'rejected':
      return `Provider rejected this ${work}`
    case 'mismatch':
      return 'Provider report differs from submitted pieces'
    case 'unavailable':
      return 'Latest storage status check is unavailable'
    case 'unknown':
      return 'Latest storage status is unknown'
    default:
      return ''
  }
}

function liveStateConfirmation(state: TaskDiagnosticLiveState, work: string) {
  switch (state) {
    case 'pending':
      return `Provider has not confirmed this ${work} yet`
    case 'confirmed':
      return `Provider confirmed this ${work}`
    case 'rejected':
      return `Provider rejected this ${work}`
    case 'mismatch':
      return 'Provider report differs from submitted pieces'
    case 'unavailable':
      return 'Latest storage status check is unavailable'
    case 'unknown':
      return 'Latest storage status is unknown'
    case 'skipped':
      return 'Latest storage status check was not run'
    default:
      return humanize(state)
  }
}

function transactionFact(label: string, transactionID: string): TaskDiagnosticFactRow {
  return {
    label,
    value: transactionID,
    monospace: true,
    detail: true,
  }
}

function addPrimaryIssue(rows: TaskDiagnosticFactRow[], label: string, value?: string) {
  if (!value) return
  rows.push({ label, value: 'Recorded error available', detail: true, detailValue: value })
}

function addFact(
  rows: TaskDiagnosticFactRow[],
  label: string,
  value: string | number | boolean | null | undefined,
  options: Pick<TaskDiagnosticFactRow, 'monospace' | 'detail' | 'displayMaxLength'> = {}
) {
  if (value === undefined || value === null || value === '') return
  rows.push({ label, value: String(value), ...options })
}

function retryLabel(retryCount?: number, maxRetries?: number) {
  if (retryCount === undefined && maxRetries === undefined) return ''
  return `${retryCount ?? 0} of ${maxRetries ?? 0}`
}

function numberValue(value?: number) {
  return value === undefined || value === 0 ? '' : String(value)
}

function recordedNumberValue(value?: number) {
  return value === undefined ? '' : String(value)
}

function listValue(values?: string[]) {
  if (values === undefined) return ''
  if (values.length === 0) return 'none'
  return values.join(', ')
}

function booleanStatus(value: boolean | undefined, liveState: TaskDiagnosticLiveState) {
  if (value === true) return 'true'
  if (value === false) return liveState === 'pending' ? 'pending' : 'false'
  if (liveState === 'pending') return 'pending'
  return ''
}

function formatID(value?: string | number) {
  return value === undefined || value === null ? '' : String(value)
}

function replicaLabel(index?: number) {
  if (index === undefined) return ''
  return `replica ${index + 1}`
}

function plural(count: number, singular: string) {
  return count === 1 ? singular : `${singular}s`
}

function humanize(value: string) {
  return value.replace(/_/g, ' ')
}
