import assert from 'node:assert/strict'
import test from 'node:test'
import { QueryClient } from '@tanstack/react-query'

import type { ObjectListResponse, TaskListResponse, UploadTransferProgress } from '../src/api/client.ts'
import { applyUploadProgressEventData, applyUploadStateChangedEventData } from '../src/lib/upload-progress-events.ts'

const progress: UploadTransferProgress = {
  scope: 'ingress_store',
  attempt: 1,
  uploaded_bytes: 4,
  total_bytes: 10,
  percent: 40,
  done: false,
  updated_at: '2026-05-06T00:00:00Z',
}

test('upload progress events patch object list cache by version id', () => {
  const qc = new QueryClient()
  qc.setQueryData<ObjectListResponse>(['objects', 'photos', '', '/', '', 50], {
    folders: [],
    objects: [
      {
        id: 1,
        key: 'image.jpg',
        current_version_id: 'v1',
        size: 10,
        state: 'uploading',
        status: 'uploading',
        location: { cache: true, filecoin: false },
        content_type: 'image/jpeg',
        etag: 'etag',
        created_at: '2026-05-06T00:00:00Z',
        updated_at: '2026-05-06T00:00:00Z',
      },
    ],
    has_more: false,
  })

  applyUploadProgressEventData(
    qc,
    JSON.stringify({
      topic: 'upload_progress_updated',
      upload_id: 11,
      version_id: 'v1',
      bucket_name: 'photos',
      object_key: 'image.jpg',
      progress,
    })
  )

  const data = qc.getQueryData<ObjectListResponse>(['objects', 'photos', '', '/', '', 50])
  assert.equal(data?.objects[0]?.progress?.percent, 40)
})

test('upload progress events patch task list cache and ignore stale attempts', () => {
  const qc = new QueryClient()
  qc.setQueryData<TaskListResponse>(['tasks', 'upload', 'ingress_store', 'running', 20, 0], {
    tasks: [
      {
        id: 7,
        type: 'upload',
        stage: 'ingress_store',
        upload_id: 11,
        ref_type: 'object',
        ref_id: 1,
        ref_version_id: 'v1',
        status: 'running',
        retry_count: 0,
        max_retries: 5,
        scheduled_at: '2026-05-06T00:00:00Z',
        progress: { ...progress, attempt: 2, uploaded_bytes: 8, percent: 80, updated_at: '2026-05-06T00:00:02Z' },
      },
    ],
    total: 1,
    limit: 20,
    offset: 0,
  })

  applyUploadProgressEventData(
    qc,
    JSON.stringify({
      topic: 'upload_progress_updated',
      upload_id: 11,
      task_id: 7,
      version_id: 'v1',
      progress: { ...progress, attempt: 1, uploaded_bytes: 10, percent: 100, updated_at: '2026-05-06T00:00:03Z' },
    })
  )

  let data = qc.getQueryData<TaskListResponse>(['tasks', 'upload', 'ingress_store', 'running', 20, 0])
  assert.equal(data?.tasks[0]?.progress?.attempt, 2)
  assert.equal(data?.tasks[0]?.progress?.uploaded_bytes, 8)

  applyUploadProgressEventData(
    qc,
    JSON.stringify({
      topic: 'upload_progress_updated',
      upload_id: 11,
      task_id: 7,
      version_id: 'v1',
      progress: { ...progress, attempt: 3, uploaded_bytes: 0, percent: 0, updated_at: '2026-05-06T00:00:04Z' },
    })
  )

  data = qc.getQueryData<TaskListResponse>(['tasks', 'upload', 'ingress_store', 'running', 20, 0])
  assert.equal(data?.tasks[0]?.progress?.attempt, 3)
  assert.equal(data?.tasks[0]?.progress?.uploaded_bytes, 0)
})

test('upload progress events keep completed progress when late running events arrive', () => {
  const qc = new QueryClient()
  qc.setQueryData<TaskListResponse>(['tasks', 'upload', 'ingress_store', 'running', 20, 0], {
    tasks: [
      {
        id: 7,
        type: 'upload',
        stage: 'ingress_store',
        upload_id: 11,
        ref_type: 'object',
        ref_id: 1,
        ref_version_id: 'v1',
        status: 'running',
        retry_count: 0,
        max_retries: 5,
        scheduled_at: '2026-05-06T00:00:00Z',
        progress: {
          ...progress,
          uploaded_bytes: 10,
          percent: 100,
          done: true,
          updated_at: '2026-05-06T00:00:02Z',
        },
      },
    ],
    total: 1,
    limit: 20,
    offset: 0,
  })

  applyUploadProgressEventData(
    qc,
    JSON.stringify({
      topic: 'upload_progress_updated',
      upload_id: 11,
      task_id: 7,
      version_id: 'v1',
      progress: {
        ...progress,
        uploaded_bytes: 10,
        percent: 100,
        done: false,
        updated_at: '2026-05-06T00:00:02Z',
      },
    })
  )

  const data = qc.getQueryData<TaskListResponse>(['tasks', 'upload', 'ingress_store', 'running', 20, 0])
  assert.equal(data?.tasks[0]?.progress?.done, true)
})

test('upload state changed events invalidate related cached queries', () => {
  const qc = new QueryClient()
  const key = ['objects', 'photos', '', '/', '', 50]
  qc.setQueryData<ObjectListResponse>(key, { folders: [], objects: [], has_more: false })

  applyUploadStateChangedEventData(
    qc,
    JSON.stringify({
      topic: 'upload_state_changed',
      upload_id: 11,
      version_id: 'v1',
      bucket_name: 'photos',
      object_key: 'image.jpg',
    })
  )

  const query = qc.getQueryCache().find({ queryKey: key })
  assert.equal(query?.state.isInvalidated, true)
})
