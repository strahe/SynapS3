import assert from 'node:assert/strict'
import test from 'node:test'

import type { TaskDiagnostic } from '../src/api/client.ts'
import {
  buildTaskDiagnosticViewModel,
  shouldRefreshTaskDiagnostic,
  taskDiagnosticReasonLabel,
  taskDiagnosticStateLabel,
  taskDiagnosticStateTone,
} from '../src/lib/task-diagnostics.ts'

test('task diagnostic labels translate backend codes without exposing chain wording by default', () => {
  assert.equal(taskDiagnosticStateLabel('waiting_for_chain'), 'Waiting for confirmation')
  assert.equal(taskDiagnosticStateLabel('confirmed'), 'Confirmed')
  assert.equal(taskDiagnosticStateTone('confirmed'), 'success')
  assert.equal(taskDiagnosticStateTone('rejected'), 'danger')
  assert.equal(taskDiagnosticStateTone('waiting_for_chain'), 'info')
  assert.equal(taskDiagnosticReasonLabel('task_piece_status_mismatch'), 'piece confirmation mismatch')
})

test('task diagnostic view model presents pending add-pieces evidence as product facts', () => {
  const view = buildTaskDiagnosticViewModel(pendingAddPiecesDiagnostic())
  const primaryText = rowsText(view.primaryFacts)

  assert.equal(view.title, 'Waiting for storage confirmation')
  assert.deepEqual(
    view.primaryFacts.slice(0, 5).map((row) => [row.label, row.value]),
    [
      ['Provider', 'Provider #2 is reachable'],
      ['Storage target', 'Data set #13778 is ready'],
      ['Pieces', '1 piece submitted'],
      ['Confirmation', 'Provider has not confirmed this storage update yet'],
      ['Storage update transaction', '0xfed1b6ba439b372edc10dce78ae900'],
    ]
  )

  assert.equal(primaryText.includes('pending / pending'), false)
  assert.equal(primaryText.includes('added -'), false)
  assert.equal(primaryText.includes('added —'), false)
  assert.equal(primaryText.includes('Provider 2 / available'), false)
  assert.equal(primaryText.includes('Add-pieces transaction'), false)
  assert.equal(primaryText.includes('add pieces'), false)
  assert.equal(primaryText.includes('chain'), false)
  assert.equal(primaryText.includes('PDP'), false)
  assert.equal(primaryText.includes('txStatus'), false)
  assert.equal(view.primaryFacts.filter((row) => row.label === 'Transaction').length, 0)
})

test('task diagnostic detail facts retain raw provider status fields', () => {
  const view = buildTaskDiagnosticViewModel(pendingAddPiecesDiagnostic())
  const clientDataSetID = factRow(view.detailFacts, 'Client data set ID')

  assert.equal(factValue(view.detailFacts, 'Task ID'), '212')
  assert.equal(factValue(view.detailFacts, 'Upload ID'), '25')
  assert.equal(factValue(view.detailFacts, 'Copy provider ID'), '2')
  assert.equal(factValue(view.detailFacts, 'Data set record'), '30')
  assert.equal(factValue(view.detailFacts, 'Data set chain ID'), '13778')
  assert.equal(clientDataSetID.value, 'client-data-set-0123456789abcdef')
  assert.equal(clientDataSetID.detail, true)
  assert.equal(clientDataSetID.displayMaxLength, 18)
  assert.equal(factValue(view.detailFacts, 'Latest status check'), 'pending')
  assert.equal(factValue(view.detailFacts, 'Recorded transaction status'), 'pending')
  assert.equal(factValue(view.detailFacts, 'Piece count'), '1')
  assert.equal(factValue(view.detailFacts, 'Provider reported pieces added'), 'pending')
  assert.equal(factValue(view.detailFacts, 'Provider status'), 'available')
  assert.equal(factValue(view.detailFacts, 'Transaction status URL'), 'https://provider.example/status/add-pieces')
  assert.equal(factValue(view.detailFacts, 'Data-set creation transaction'), '0x8ae093babe9649d0cf1cfc5b916e000000')
  assert.equal(factValue(view.detailFacts, 'Add-pieces transaction'), '0xfed1b6ba439b372edc10dce78ae900')
  assert.equal(view.detailFacts.filter((row) => row.label === 'Transaction').length, 0)
})

test('task diagnostic detail facts preserve zero and empty live evidence', () => {
  const diagnostic = pendingAddPiecesDiagnostic()
  if (!diagnostic.evidence.live_check) throw new Error('expected live check fixture')
  diagnostic.current_state = 'mismatch'
  diagnostic.evidence.live_check.state = 'mismatch'
  diagnostic.evidence.live_check.piece_count = 0
  diagnostic.evidence.live_check.confirmed_piece_ids = []

  const view = buildTaskDiagnosticViewModel(diagnostic)

  assert.equal(factValue(view.detailFacts, 'Piece count'), '0')
  assert.equal(factValue(view.detailFacts, 'Confirmed piece IDs'), 'none')
})

test('task diagnostic submitted pieces use task evidence when provider count differs', () => {
  const diagnostic = pendingAddPiecesDiagnostic()
  if (!diagnostic.evidence.live_check) throw new Error('expected live check fixture')
  diagnostic.current_state = 'mismatch'
  diagnostic.evidence.live_check.state = 'mismatch'
  diagnostic.evidence.live_check.piece_count = 2

  const view = buildTaskDiagnosticViewModel(diagnostic)

  assert.equal(factValue(view.primaryFacts, 'Pieces'), '1 piece submitted')
  assert.equal(factValue(view.detailFacts, 'Piece count'), '2')
})

test('task diagnostic view model formats state-specific titles', () => {
  assert.equal(buildTaskDiagnosticViewModel(pendingAddPiecesDiagnostic()).title, 'Waiting for storage confirmation')
  assert.equal(stateTitle({ current_state: 'confirmed', live_state: 'confirmed' }), 'Storage confirmation received')
  assert.equal(stateTitle({ current_state: 'rejected', live_state: 'rejected' }), 'Storage confirmation was rejected')
  assert.equal(
    stateTitle({ current_state: 'mismatch', live_state: 'mismatch' }),
    'Provider confirmation does not match the submitted work'
  )
  assert.equal(
    stateTitle({ current_state: 'unavailable', live_state: 'unavailable' }),
    'Storage status check is unavailable'
  )
})

test('task diagnostic refresh only runs when live status evidence is checkable', () => {
  assert.equal(shouldRefreshTaskDiagnostic(pendingAddPiecesDiagnostic()), true)

  const preparing = diagnosticWith({
    operation: 'prepare_upload',
    transaction: undefined,
  })
  assert.equal(shouldRefreshTaskDiagnostic(preparing), false)

  const transfer = diagnosticWith({
    operation: 'transfer_piece',
    transaction: undefined,
  })
  assert.equal(shouldRefreshTaskDiagnostic(transfer), false)

  const createWithoutStatusURL = diagnosticWith({
    operation: 'create_data_set',
    transaction: { kind: 'create_data_set' },
  })
  assert.equal(shouldRefreshTaskDiagnostic(createWithoutStatusURL), false)

  const createWithStatusURL = diagnosticWith({
    operation: 'create_data_set',
    transaction: { kind: 'create_data_set', status_url: 'https://provider.example/status/create' },
  })
  assert.equal(shouldRefreshTaskDiagnostic(createWithStatusURL), true)

  const addPiecesWithBuildableURL = diagnosticWith({
    operation: 'add_pieces',
    transaction: {
      kind: 'add_pieces',
      service_url: 'https://provider.example',
      data_set_id: '13778',
      transaction_id: '0xfed1b6ba439b372edc10dce78ae900',
    },
  })
  assert.equal(shouldRefreshTaskDiagnostic(addPiecesWithBuildableURL), true)
})

function pendingAddPiecesDiagnostic(): TaskDiagnostic {
  return {
    checked_at: '2026-05-22T10:00:00Z',
    current_state: 'waiting_for_chain',
    signal: {
      status: 'degraded',
      level: 'warning',
      reason_codes: ['task_chain_pending'],
      freshness: { stale: false, warnings: [] },
    },
    reason_codes: ['task_chain_pending'],
    next_action: 'wait',
    evidence: {
      operation: 'add_pieces',
      task: {
        id: 212,
        type: 'upload',
        stage: 'ingress_commit',
        status: 'waiting',
        retry_count: 0,
        max_retries: 5,
      },
      upload: { id: 25, status: 'ingress_ready', requested_copies: 3 },
      copy: {
        upload_id: 25,
        copy_index: 0,
        status: 'committing',
        provider_id: '2',
        chain_data_set_id: '13778',
        commit_transaction_id: '0xfed1b6ba439b372edc10dce78ae900',
      },
      data_set: {
        id: 30,
        status: 'ready',
        provider_id: '2',
        copy_index: 0,
        chain_data_set_id: '13778',
        client_data_set_id: 'client-data-set-0123456789abcdef',
        create_transaction_id: '0x8ae093babe9649d0cf1cfc5b916e000000',
      },
      provider: { provider_id: '2', status: 'available', service_url: 'https://provider.example' },
      transaction: {
        kind: 'add_pieces',
        status_url: 'https://provider.example/status/add-pieces',
        service_url: 'https://provider.example',
        data_set_id: '13778',
        transaction_id: '0xfed1b6ba439b372edc10dce78ae900',
        piece_count: 1,
      },
      live_check: {
        state: 'pending',
        status_url: 'https://provider.example/status/add-pieces',
        tx_status: 'pending',
        data_set_id: '13778',
        piece_count: 1,
      },
    },
  }
}

function diagnosticWith(evidence: Partial<TaskDiagnostic['evidence']>) {
  const diagnostic = structuredClone(pendingAddPiecesDiagnostic()) as TaskDiagnostic
  diagnostic.evidence = { ...diagnostic.evidence, ...evidence }
  return diagnostic
}

function stateTitle({
  current_state,
  live_state,
}: {
  current_state: TaskDiagnostic['current_state']
  live_state: NonNullable<TaskDiagnostic['evidence']['live_check']>['state']
}) {
  const diagnostic = pendingAddPiecesDiagnostic()
  diagnostic.current_state = current_state
  if (diagnostic.evidence.live_check) {
    diagnostic.evidence.live_check.state = live_state
  }
  const view = buildTaskDiagnosticViewModel(diagnostic)
  return view.title
}

function rowsText(rows: Array<{ label: string; value: string }>) {
  return rows.map((row) => `${row.label}: ${row.value}`).join('\n')
}

function factValue(rows: Array<{ label: string; value: string }>, label: string) {
  return factRow(rows, label).value
}

function factRow<T extends { label: string }>(rows: T[], label: string) {
  const row = rows.find((item) => item.label === label)
  assert.ok(row, `missing fact ${label}`)
  return row
}
