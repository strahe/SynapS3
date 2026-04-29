import { useQueryClient } from '@tanstack/react-query'
import { createFileRoute, Link, useNavigate } from '@tanstack/react-router'
import { Download, Folder, History, Loader2, RefreshCw, Trash2, UserRound } from 'lucide-react'
import { Fragment, useEffect, useState } from 'react'
import { api, internalRootOwnerAccessKey, type ObjectItem } from '@/api/client'
import { BreadcrumbCurrentPage } from '@/components/app/BreadcrumbCurrentPage'
import { BucketOwnerSelect } from '@/components/app/BucketOwnerSelect'
import { PageHeader } from '@/components/app/PageHeader'
import { bucketStatusTone, objectStateTone, StatusBadge } from '@/components/app/StatusBadge'
import {
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbLink,
  BreadcrumbList,
  BreadcrumbSeparator,
} from '@/components/ui/breadcrumb'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import {
  useBucket,
  useBucketObjects,
  useBucketObjectVersions,
  useDeleteBucket,
  useS3Users,
  useUpdateBucketOwner,
} from '@/hooks/queries'
import { formatBytes, formatNumber, timeAgo } from '@/lib/utils'

export const Route = createFileRoute('/buckets/$name')({
  component: ObjectBrowserPage,
})

function DeleteBucketDetailDialog({ bucketName, objectCount }: { bucketName: string; objectCount: number }) {
  const [open, setOpen] = useState(false)
  const [confirmName, setConfirmName] = useState('')
  const [error, setError] = useState<string | null>(null)
  const deleteBucket = useDeleteBucket()
  const navigate = useNavigate()

  const recursive = objectCount > 0
  const nameMatches = confirmName === bucketName

  const reset = () => {
    setConfirmName('')
    setError(null)
    deleteBucket.reset()
  }

  const handleOpenChange = (next: boolean) => {
    if (!next) reset()
    setOpen(next)
  }

  const handleDelete = () => {
    if (!nameMatches) return
    setError(null)
    deleteBucket.mutate(
      { name: bucketName, recursive },
      {
        onSuccess: () => {
          setOpen(false)
          reset()
          navigate({ to: '/buckets' })
        },
        onError: (mutationError) => {
          setError(mutationError instanceof Error ? mutationError.message : 'Failed to delete bucket')
        },
      }
    )
  }

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogTrigger asChild>
        <Button variant="destructive" size="sm">
          <Trash2 data-icon="inline-start" />
          Delete
        </Button>
      </DialogTrigger>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Delete bucket "{bucketName}"</DialogTitle>
          <DialogDescription>
            {recursive
              ? `This will recursively purge ${formatNumber(objectCount)} object(s) and their cached data. Deletion is blocked while lifecycle tasks, object processing, or multipart uploads are in flight.`
              : 'This empty bucket will be marked for deletion and its proof set removed on-chain. Deletion is blocked while lifecycle tasks or multipart uploads are in flight.'}
          </DialogDescription>
        </DialogHeader>
        <div className="flex flex-col gap-2">
          <Label htmlFor="confirm-delete-detail">
            Type <span className="font-mono font-semibold">{bucketName}</span> to confirm
          </Label>
          <Input
            id="confirm-delete-detail"
            value={confirmName}
            onChange={(e) => setConfirmName(e.target.value)}
            placeholder={bucketName}
            autoFocus
            disabled={deleteBucket.isPending}
          />
        </div>
        {error && <p className="text-sm text-destructive">{error}</p>}
        <DialogFooter>
          <Button
            type="button"
            variant="outline"
            onClick={() => handleOpenChange(false)}
            disabled={deleteBucket.isPending}
          >
            Cancel
          </Button>
          <Button variant="destructive" onClick={handleDelete} disabled={!nameMatches || deleteBucket.isPending}>
            {deleteBucket.isPending && <Loader2 data-icon="inline-start" className="animate-spin" />}
            Delete bucket
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

function ChangeBucketOwnerDetailDialog({
  bucketName,
  ownerAccessKey,
}: {
  bucketName: string
  ownerAccessKey: string | null
}) {
  const [open, setOpen] = useState(false)
  const [selectedOwner, setSelectedOwner] = useState(ownerAccessKey ?? '')
  const [error, setError] = useState<string | null>(null)
  const { data: users = [], isLoading: usersLoading, error: usersError } = useS3Users()
  const updateOwner = useUpdateBucketOwner()

  useEffect(() => {
    if (!open) {
      setSelectedOwner(ownerAccessKey ?? '')
      setError(null)
    }
  }, [ownerAccessKey, open])

  const reset = () => {
    setSelectedOwner(ownerAccessKey ?? '')
    setError(null)
    updateOwner.reset()
  }

  const handleOpenChange = (next: boolean) => {
    reset()
    setOpen(next)
  }

  const handleUpdate = () => {
    if (!selectedOwner || selectedOwner === ownerAccessKey) return
    setError(null)
    updateOwner.mutate(
      { name: bucketName, ownerAccessKey: selectedOwner },
      {
        onSuccess: () => {
          setOpen(false)
          reset()
        },
        onError: (mutationError) => {
          setError(mutationError instanceof Error ? mutationError.message : 'Failed to update owner')
        },
      }
    )
  }

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogTrigger asChild>
        <Button variant="outline" size="sm">
          <UserRound data-icon="inline-start" />
          {ownerAccessKey ? 'Change owner' : 'Assign owner'}
        </Button>
      </DialogTrigger>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>{ownerAccessKey ? 'Change bucket owner' : 'Assign bucket owner'}</DialogTitle>
          <DialogDescription>Transfer full control of "{bucketName}" to an existing S3 user.</DialogDescription>
        </DialogHeader>
        <div className="flex flex-col gap-2">
          <Label htmlFor="bucket-detail-owner">Owner</Label>
          <BucketOwnerSelect
            id="bucket-detail-owner"
            value={selectedOwner}
            onChange={setSelectedOwner}
            disabled={updateOwner.isPending || usersLoading}
            users={users}
          />
          {users.length === 0 && !usersLoading && (
            <p className="text-xs text-muted-foreground">
              No S3 users yet. Internal root can be used as fallback owner.
            </p>
          )}
          {usersError && <p className="text-xs text-destructive">Failed to load S3 users.</p>}
        </div>
        {error && <p className="text-sm text-destructive">{error}</p>}
        <DialogFooter>
          <Button
            type="button"
            variant="outline"
            onClick={() => handleOpenChange(false)}
            disabled={updateOwner.isPending}
          >
            Cancel
          </Button>
          <Button
            type="button"
            onClick={handleUpdate}
            disabled={!selectedOwner || selectedOwner === ownerAccessKey || updateOwner.isPending}
          >
            {updateOwner.isPending && <Loader2 data-icon="inline-start" className="animate-spin" />}
            Save owner
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

function ObjectVersionsDialog({ bucketName, object }: { bucketName: string; object: ObjectItem }) {
  const [open, setOpen] = useState(false)
  const [versionMarker, setVersionMarker] = useState('')
  const versions = useBucketObjectVersions(bucketName, object.key, versionMarker, 50, open)

  useEffect(() => {
    if (open) setVersionMarker('')
  }, [open])

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button variant="ghost" size="icon-sm" aria-label={`Versions for ${object.key}`} title="Versions">
          <History />
        </Button>
      </DialogTrigger>
      <DialogContent className="w-[calc(100vw-2rem)] max-w-[calc(100vw-2rem)] sm:max-w-6xl lg:p-6">
        <DialogHeader>
          <DialogTitle>Object versions</DialogTitle>
          <DialogDescription className="pr-8">
            <span className="block max-w-full truncate font-mono text-xs" title={object.key}>
              {object.key}
            </span>
          </DialogDescription>
        </DialogHeader>
        {versions.isLoading ? (
          <div className="flex h-40 items-center justify-center">
            <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
          </div>
        ) : versions.error ? (
          <div className="text-sm text-destructive">Failed to load object versions</div>
        ) : (
          <div className="overflow-hidden rounded-md border border-border">
            <Table className="table-fixed">
              <colgroup>
                <col className="w-[19%]" />
                <col className="w-[9%]" />
                <col className="w-[12%]" />
                <col className="w-[18%]" />
                <col className="w-[20%]" />
                <col className="w-[12%]" />
                <col className="w-[10%]" />
              </colgroup>
              <TableHeader>
                <TableRow className="bg-muted/50">
                  <TableHead className="px-2">Version</TableHead>
                  <TableHead className="px-2 text-right">Size</TableHead>
                  <TableHead className="px-2">State</TableHead>
                  <TableHead className="px-2">ETag</TableHead>
                  <TableHead className="px-2">Piece CID</TableHead>
                  <TableHead className="px-2">Created</TableHead>
                  <TableHead className="px-2 text-right">Download</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {versions.data?.versions.map((version) => (
                  <TableRow key={version.version_id}>
                    <TableCell className="overflow-hidden px-2" title={version.version_id}>
                      <div className="flex min-w-0 items-center gap-2">
                        <span className="min-w-0 truncate font-mono text-xs">{version.version_id}</span>
                        {version.is_current && (
                          <StatusBadge tone="success" className="shrink-0">
                            Current
                          </StatusBadge>
                        )}
                      </div>
                    </TableCell>
                    <TableCell className="overflow-hidden px-2 text-right">{formatBytes(version.size)}</TableCell>
                    <TableCell className="overflow-hidden px-2">
                      <StatusBadge tone={objectStateTone(version.state)} className="max-w-full truncate">
                        {version.state}
                      </StatusBadge>
                    </TableCell>
                    <TableCell
                      className="overflow-hidden truncate px-2 font-mono text-xs text-muted-foreground"
                      title={version.etag}
                    >
                      {version.etag}
                    </TableCell>
                    <TableCell
                      className="overflow-hidden truncate px-2 font-mono text-xs text-muted-foreground"
                      title={version.piece_cid ?? undefined}
                    >
                      {version.piece_cid ?? '—'}
                    </TableCell>
                    <TableCell
                      className="overflow-hidden truncate px-2 text-muted-foreground"
                      title={version.created_at}
                    >
                      {timeAgo(version.created_at)}
                    </TableCell>
                    <TableCell className="px-2 text-right">
                      <Button variant="ghost" size="icon-sm" asChild>
                        <a
                          href={api.getObjectDownloadUrl(bucketName, object.key, version.version_id)}
                          aria-label={`Download ${object.key} version ${version.version_id}`}
                          title="Download"
                        >
                          <Download />
                        </a>
                      </Button>
                    </TableCell>
                  </TableRow>
                ))}
                {versions.data?.versions.length === 0 && (
                  <TableRow>
                    <TableCell colSpan={7} className="h-20 text-center text-muted-foreground">
                      No versions found
                    </TableCell>
                  </TableRow>
                )}
              </TableBody>
            </Table>
          </div>
        )}
        {versions.data?.has_more && versions.data.next_version_marker && (
          <DialogFooter>
            <Button
              variant="outline"
              size="sm"
              onClick={() => setVersionMarker(versions.data.next_version_marker ?? '')}
            >
              Next page
            </Button>
          </DialogFooter>
        )}
      </DialogContent>
    </Dialog>
  )
}

function ObjectBrowserPage() {
  const { name } = Route.useParams()
  const [prefix, setPrefix] = useState('')
  const [marker, setMarker] = useState('')

  const bucket = useBucket(name)
  const objects = useBucketObjects(name, prefix, marker)
  const qc = useQueryClient()

  const prefixParts = prefix.split('/').filter(Boolean)

  const navigateToPrefix = (newPrefix: string) => {
    setPrefix(newPrefix)
    setMarker('')
  }

  const handleRefresh = () => {
    qc.invalidateQueries({ queryKey: ['bucket', name] })
    qc.invalidateQueries({ queryKey: ['objects', name] })
  }

  const canDelete = bucket.data?.status === 'active'

  const folders = new Set<string>()
  const files =
    objects.data?.objects.filter((object) => {
      const rest = object.key.slice(prefix.length)
      const slashIdx = rest.indexOf('/')
      if (slashIdx >= 0) {
        folders.add(prefix + rest.substring(0, slashIdx + 1))
        return false
      }
      return true
    }) ?? []

  return (
    <div className="flex flex-col gap-4 p-6">
      <BucketBreadcrumb name={name} prefixParts={prefixParts} navigateToPrefix={navigateToPrefix} />

      <PageHeader
        title={name}
        meta={
          bucket.data && <StatusBadge tone={bucketStatusTone(bucket.data.status)}>{bucket.data.status}</StatusBadge>
        }
        actions={
          <>
            {bucket.data && (
              <ChangeBucketOwnerDetailDialog bucketName={name} ownerAccessKey={bucket.data.owner_access_key} />
            )}
            {canDelete ? (
              <DeleteBucketDetailDialog bucketName={name} objectCount={bucket.data?.object_count ?? 0} />
            ) : (
              <Button variant="destructive" size="sm" disabled>
                <Trash2 data-icon="inline-start" />
                Delete
              </Button>
            )}
            <Button variant="outline" size="sm" onClick={handleRefresh}>
              <RefreshCw data-icon="inline-start" /> Refresh
            </Button>
          </>
        }
      />

      {bucket.isLoading ? (
        <div className="flex h-32 items-center justify-center">
          <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
        </div>
      ) : bucket.error ? (
        <div className="text-destructive">Failed to load bucket details</div>
      ) : bucket.data ? (
        <Card>
          <CardHeader>
            <CardTitle>Bucket details</CardTitle>
          </CardHeader>
          <CardContent>
            <dl className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
              <div>
                <dt className="text-sm text-muted-foreground">Owner</dt>
                <dd className="text-sm font-medium">
                  {bucket.data.owner_access_key === internalRootOwnerAccessKey ? (
                    <StatusBadge tone="neutral">Internal root</StatusBadge>
                  ) : bucket.data.owner_access_key ? (
                    <code className="text-xs text-muted-foreground">{bucket.data.owner_access_key}</code>
                  ) : (
                    <StatusBadge tone="warning">Unassigned</StatusBadge>
                  )}
                </dd>
              </div>
              <div>
                <dt className="text-sm text-muted-foreground">Proof Set</dt>
                <dd className="font-mono text-xs text-muted-foreground">{bucket.data.proof_set_id ?? '—'}</dd>
              </div>
              <div>
                <dt className="text-sm text-muted-foreground">Objects</dt>
                <dd className="text-sm font-medium">{formatNumber(bucket.data.object_count)}</dd>
              </div>
              <div>
                <dt className="text-sm text-muted-foreground">Total size</dt>
                <dd className="text-sm font-medium">{formatBytes(bucket.data.total_size_bytes)}</dd>
              </div>
              <div>
                <dt className="text-sm text-muted-foreground">Versioning</dt>
                <dd className="text-sm font-medium">
                  <StatusBadge tone="success">{bucket.data.versioning_status}</StatusBadge>
                </dd>
              </div>
              <div>
                <dt className="text-sm text-muted-foreground">Created</dt>
                <dd className="text-sm text-muted-foreground" title={bucket.data.created_at}>
                  {timeAgo(bucket.data.created_at)}
                </dd>
              </div>
              <div>
                <dt className="text-sm text-muted-foreground">Updated</dt>
                <dd className="text-sm text-muted-foreground" title={bucket.data.updated_at}>
                  {timeAgo(bucket.data.updated_at)}
                </dd>
              </div>
              <div>
                <dt className="text-sm text-muted-foreground">Current path</dt>
                <dd className="font-mono text-xs text-muted-foreground">{prefix || '/'}</dd>
              </div>
            </dl>
          </CardContent>
        </Card>
      ) : null}

      {objects.isLoading ? (
        <div className="flex h-60 items-center justify-center">
          <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
        </div>
      ) : objects.error ? (
        <div className="text-destructive">Failed to load objects</div>
      ) : (
        <>
          <div className="overflow-hidden rounded-lg border border-border">
            <Table>
              <TableHeader>
                <TableRow className="bg-muted/50">
                  <TableHead className="px-4">Key</TableHead>
                  <TableHead className="px-4 text-right">Size</TableHead>
                  <TableHead className="px-4">State</TableHead>
                  <TableHead className="px-4">Type</TableHead>
                  <TableHead className="px-4">Current Version</TableHead>
                  <TableHead className="px-4">ETag</TableHead>
                  <TableHead className="px-4">Piece CID</TableHead>
                  <TableHead className="px-4">Created</TableHead>
                  <TableHead className="px-4">Updated</TableHead>
                  <TableHead className="px-4 text-right">Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {[...folders].sort().map((folder) => (
                  <TableRow key={folder}>
                    <TableCell className="px-4">
                      <Button
                        type="button"
                        variant="link"
                        size="sm"
                        className="h-auto p-0"
                        onClick={() => navigateToPrefix(folder)}
                      >
                        <Folder data-icon="inline-start" />
                        {folder.slice(prefix.length)}
                      </Button>
                    </TableCell>
                    <TableCell className="px-4 text-right text-muted-foreground">—</TableCell>
                    <TableCell className="px-4">—</TableCell>
                    <TableCell className="px-4">—</TableCell>
                    <TableCell className="px-4">—</TableCell>
                    <TableCell className="px-4">—</TableCell>
                    <TableCell className="px-4">—</TableCell>
                    <TableCell className="px-4">—</TableCell>
                    <TableCell className="px-4">—</TableCell>
                    <TableCell className="px-4 text-right">—</TableCell>
                  </TableRow>
                ))}
                {files.map((object) => (
                  <TableRow key={object.id}>
                    <TableCell className="px-4 font-mono text-xs">{object.key.slice(prefix.length)}</TableCell>
                    <TableCell className="px-4 text-right">{formatBytes(object.size)}</TableCell>
                    <TableCell className="px-4">
                      <StatusBadge tone={objectStateTone(object.state)}>{object.state}</StatusBadge>
                    </TableCell>
                    <TableCell className="px-4 text-muted-foreground">{object.content_type}</TableCell>
                    <TableCell
                      className="max-w-52 truncate px-4 font-mono text-xs text-muted-foreground"
                      title={object.current_version_id}
                    >
                      {object.current_version_id}
                    </TableCell>
                    <TableCell
                      className="max-w-40 truncate px-4 font-mono text-xs text-muted-foreground"
                      title={object.etag}
                    >
                      {object.etag}
                    </TableCell>
                    <TableCell
                      className="max-w-52 truncate px-4 font-mono text-xs text-muted-foreground"
                      title={object.piece_cid ?? undefined}
                    >
                      {object.piece_cid ?? '—'}
                    </TableCell>
                    <TableCell className="px-4 text-muted-foreground" title={object.created_at}>
                      {timeAgo(object.created_at)}
                    </TableCell>
                    <TableCell className="px-4 text-muted-foreground" title={object.updated_at}>
                      {timeAgo(object.updated_at)}
                    </TableCell>
                    <TableCell className="px-4 text-right">
                      <div className="flex justify-end gap-1">
                        <ObjectVersionsDialog bucketName={name} object={object} />
                        <Button variant="ghost" size="icon-sm" asChild>
                          <a
                            href={api.getObjectDownloadUrl(name, object.key)}
                            aria-label={`Download ${object.key}`}
                            title="Download"
                          >
                            <Download />
                          </a>
                        </Button>
                      </div>
                    </TableCell>
                  </TableRow>
                ))}
                {folders.size === 0 && files.length === 0 && (
                  <TableRow>
                    <TableCell colSpan={10} className="h-24 text-center text-muted-foreground">
                      No objects found
                    </TableCell>
                  </TableRow>
                )}
              </TableBody>
            </Table>
          </div>

          <div className="flex justify-between">
            {marker && (
              <Button variant="outline" size="sm" onClick={() => setMarker('')}>
                First page
              </Button>
            )}
            {objects.data?.has_more && objects.data.next_marker && (
              <Button
                variant="outline"
                size="sm"
                className="ml-auto"
                onClick={() => {
                  if (objects.data?.next_marker) {
                    setMarker(objects.data.next_marker)
                  }
                }}
              >
                Next page
              </Button>
            )}
          </div>
        </>
      )}
    </div>
  )
}

function BucketBreadcrumb({
  name,
  prefixParts,
  navigateToPrefix,
}: {
  name: string
  prefixParts: string[]
  navigateToPrefix: (prefix: string) => void
}) {
  return (
    <Breadcrumb>
      <BreadcrumbList>
        <BreadcrumbItem>
          <BreadcrumbLink asChild>
            <Link to="/buckets">Buckets</Link>
          </BreadcrumbLink>
        </BreadcrumbItem>
        <BreadcrumbSeparator />
        <BreadcrumbItem>
          {prefixParts.length > 0 ? (
            <BreadcrumbLink asChild>
              <Button
                type="button"
                variant="link"
                className="h-auto p-0 text-sm font-normal"
                onClick={() => navigateToPrefix('')}
              >
                {name}
              </Button>
            </BreadcrumbLink>
          ) : (
            <BreadcrumbCurrentPage>{name}</BreadcrumbCurrentPage>
          )}
        </BreadcrumbItem>
        {prefixParts.map((part, index) => {
          const targetPrefix = `${prefixParts.slice(0, index + 1).join('/')}/`
          const isLast = index === prefixParts.length - 1

          return (
            <Fragment key={targetPrefix}>
              <BreadcrumbSeparator />
              <BreadcrumbItem>
                {isLast ? (
                  <BreadcrumbCurrentPage>{part}</BreadcrumbCurrentPage>
                ) : (
                  <BreadcrumbLink asChild>
                    <Button
                      type="button"
                      variant="link"
                      className="h-auto p-0 text-sm font-normal"
                      onClick={() => navigateToPrefix(targetPrefix)}
                    >
                      {part}
                    </Button>
                  </BreadcrumbLink>
                )}
              </BreadcrumbItem>
            </Fragment>
          )
        })}
      </BreadcrumbList>
    </Breadcrumb>
  )
}
