import type { QueryClient } from '@tanstack/react-query'
import type {
  ObjectListResponse,
  ObjectProvenance,
  ObjectStatusDetail,
  ObjectVersionListResponse,
  TaskListResponse,
  TaskRefDetail,
  UploadTransferProgress,
} from '@/api/client'

interface UploadProgressEventPayload {
  topic?: string
  upload_id?: number
  task_id?: number
  version_id?: string
  bucket_name?: string
  object_key?: string
  progress?: UploadTransferProgress
}

interface UploadStateChangedEventPayload {
  topic?: string
  upload_id?: number
  task_id?: number
  version_id?: string
  bucket_name?: string
  object_key?: string
}

export function applyUploadProgressEventData(queryClient: QueryClient, raw: string) {
  let payload: UploadProgressEventPayload
  try {
    payload = JSON.parse(raw) as UploadProgressEventPayload
  } catch {
    return
  }
  if (payload.topic !== 'upload_progress_updated' || !payload.version_id || !payload.progress) {
    return
  }
  applyUploadProgressUpdate(queryClient, payload)
}

export function applyUploadStateChangedEventData(queryClient: QueryClient, raw: string) {
  let payload: UploadStateChangedEventPayload
  try {
    payload = JSON.parse(raw) as UploadStateChangedEventPayload
  } catch {
    return
  }
  if (payload.topic !== 'upload_state_changed') return

  if (payload.bucket_name) {
    queryClient.invalidateQueries({ queryKey: ['objects', payload.bucket_name] })
    if (payload.object_key) {
      queryClient.invalidateQueries({ queryKey: ['objectVersions', payload.bucket_name, payload.object_key] })
    }
    if (payload.version_id) {
      queryClient.invalidateQueries({ queryKey: ['objectStatusDetail', payload.bucket_name, payload.version_id] })
      queryClient.invalidateQueries({ queryKey: ['objectProvenance', payload.bucket_name, payload.version_id] })
    }
  }
  queryClient.invalidateQueries({ queryKey: ['tasks'] })
  queryClient.invalidateQueries({ queryKey: ['taskRefDetail'] })
}

export function applyUploadProgressUpdate(queryClient: QueryClient, payload: UploadProgressEventPayload) {
  const progress = payload.progress
  if (!payload.version_id || !progress) return

  queryClient.setQueriesData<ObjectListResponse>({ queryKey: ['objects'] }, (data) => {
    if (!data?.objects.length) return data
    let changed = false
    const objects = data.objects.map((object) => {
      if (object.current_version_id !== payload.version_id) return object
      const next = mergeProgress(object.progress, progress)
      if (next === object.progress) return object
      changed = true
      return { ...object, progress: next }
    })
    return changed ? { ...data, objects } : data
  })

  queryClient.setQueriesData<ObjectVersionListResponse>({ queryKey: ['objectVersions'] }, (data) => {
    if (!data?.versions.length) return data
    let changed = false
    const versions = data.versions.map((version) => {
      if (version.version_id !== payload.version_id) return version
      const next = mergeProgress(version.progress, progress)
      if (next === version.progress) return version
      changed = true
      return { ...version, progress: next }
    })
    return changed ? { ...data, versions } : data
  })

  queryClient.setQueriesData<ObjectStatusDetail>({ queryKey: ['objectStatusDetail'] }, (data) => {
    if (!data || data.version_id !== payload.version_id) return data
    const next = mergeProgress(data.progress, progress)
    return next === data.progress ? data : { ...data, progress: next }
  })

  queryClient.setQueriesData<ObjectProvenance>({ queryKey: ['objectProvenance'] }, (data) => {
    if (!data || data.version_id !== payload.version_id) return data
    const next = mergeProgress(data.progress, progress)
    return next === data.progress ? data : { ...data, progress: next }
  })

  queryClient.setQueriesData<TaskListResponse>({ queryKey: ['tasks'] }, (data) => {
    if (!data?.tasks.length) return data
    let changed = false
    const tasks = data.tasks.map((task) => {
      const matches =
        (typeof payload.task_id === 'number' && task.id === payload.task_id) ||
        (typeof payload.upload_id === 'number' && task.upload_id === payload.upload_id) ||
        task.ref_version_id === payload.version_id
      if (!matches) return task
      const next = mergeProgress(task.progress, progress)
      if (next === task.progress) return task
      changed = true
      return { ...task, progress: next }
    })
    return changed ? { ...data, tasks } : data
  })

  queryClient.setQueriesData<TaskRefDetail>({ queryKey: ['taskRefDetail'] }, (data) => {
    if (!data?.object || data.object.version_id !== payload.version_id) return data
    const next = mergeProgress(data.object.progress, progress)
    return next === data.object.progress ? data : { ...data, object: { ...data.object, progress: next } }
  })
}

function mergeProgress(current: UploadTransferProgress | undefined, next: UploadTransferProgress) {
  if (!current) return next
  if (next.attempt > current.attempt) return next
  if (next.attempt < current.attempt) return current
  if (current.done && !next.done) return current
  if (next.uploaded_bytes < current.uploaded_bytes) return current
  if (next.updated_at < current.updated_at && next.uploaded_bytes === current.uploaded_bytes) return current
  return next
}
