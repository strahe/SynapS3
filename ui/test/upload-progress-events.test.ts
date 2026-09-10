import assert from 'node:assert/strict'
import test from 'node:test'
import { QueryClient } from '@tanstack/react-query'

import type { ObjectListResponse, UploadTransferProgress } from '../src/api/client.ts'
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
  qc.setQueryData(['tasks'], { items: [] })
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
  assert.equal(qc.getQueryCache().find({ queryKey: ['tasks'] })?.state.isInvalidated, false)
})

test('upload progress events ignore stale attempts and late running updates', () => {
  const qc = new QueryClient()
  const key = ['objects', 'photos', '', '/', '', 50]
  qc.setQueryData<ObjectListResponse>(key, {
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
        progress: {
          ...progress,
          attempt: 2,
          uploaded_bytes: 10,
          percent: 100,
          done: true,
          updated_at: '2026-05-06T00:00:02Z',
        },
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
      progress: { ...progress, attempt: 1, uploaded_bytes: 10, percent: 100, updated_at: '2026-05-06T00:00:03Z' },
    })
  )

  let data = qc.getQueryData<ObjectListResponse>(key)
  assert.equal(data?.objects[0]?.progress?.attempt, 2)
  assert.equal(data?.objects[0]?.progress?.done, true)

  applyUploadProgressEventData(
    qc,
    JSON.stringify({
      topic: 'upload_progress_updated',
      upload_id: 11,
      version_id: 'v1',
      progress: {
        ...progress,
        attempt: 2,
        uploaded_bytes: 10,
        percent: 100,
        done: false,
        updated_at: '2026-05-06T00:00:03Z',
      },
    })
  )

  data = qc.getQueryData<ObjectListResponse>(key)
  assert.equal(data?.objects[0]?.progress?.done, true)
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

test('upload state changed events without object identity invalidate upload views', () => {
  const qc = new QueryClient()
  const keys = [
    ['objects', 'photos'],
    ['objectVersions', 'photos', 'image.jpg'],
    ['objectStatusDetail', 'photos', 'v1'],
    ['objectProvenance', 'photos', 'v1'],
  ]
  for (const key of keys) qc.setQueryData(key, {})

  applyUploadStateChangedEventData(qc, JSON.stringify({ topic: 'upload_state_changed', task_id: 12 }))

  for (const key of keys) {
    assert.equal(qc.getQueryCache().find({ queryKey: key })?.state.isInvalidated, true)
  }
})
