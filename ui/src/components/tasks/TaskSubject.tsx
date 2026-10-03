import { useRef, useState } from 'react'
import { APIError, type TaskItem, type TaskSubjectInfo } from '@/api/client'
import { CopyableValue } from '@/components/app/CopyableValue'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import { useTaskSubject } from '@/hooks/queries'
import { isTaskSubjectType, taskSubjectFields, taskSubjectLabel, taskSystemDescription } from '@/lib/tasks'

type Overlay = 'tooltip' | 'popover' | null

export function TaskSubject({ task }: { task: TaskItem }) {
  const [overlay, setOverlay] = useState<Overlay>(null)
  const overlayRef = useRef<Overlay>(null)
  const openedOnPointerDownRef = useRef(false)
  const suppressFocusRef = useRef(false)
  const label = taskSubjectLabel(task)
  const system = task.subject_type === 'system'
  const queryable = task.subject_type && task.subject_key && isTaskSubjectType(task.subject_type)

  const changeOverlay = (value: Overlay) => {
    overlayRef.current = value
    setOverlay(value)
  }

  if (!system && !queryable) return <span>{label}</span>

  return (
    <TooltipProvider delayDuration={200} skipDelayDuration={0}>
      <Popover open={overlay === 'popover'} onOpenChange={(open) => changeOverlay(open ? 'popover' : null)}>
        <Tooltip
          open={overlay === 'tooltip'}
          onOpenChange={(open) => {
            if (overlayRef.current === 'popover') return
            if (open && suppressFocusRef.current) {
              suppressFocusRef.current = false
              return
            }
            changeOverlay(open ? 'tooltip' : null)
          }}
        >
          <PopoverTrigger asChild>
            <TooltipTrigger asChild>
              <button
                type="button"
                className="whitespace-nowrap underline decoration-dotted underline-offset-4 outline-offset-4"
                onPointerDownCapture={(event) => {
                  openedOnPointerDownRef.current = event.button === 0 && overlayRef.current === 'tooltip'
                  if (openedOnPointerDownRef.current) {
                    changeOverlay('popover')
                  }
                }}
                onPointerCancel={() => {
                  openedOnPointerDownRef.current = false
                }}
                onClick={(event) => {
                  event.preventDefault()
                  const openedOnPointerDown = event.detail > 0 && openedOnPointerDownRef.current
                  openedOnPointerDownRef.current = false
                  if (openedOnPointerDown) return
                  changeOverlay(overlayRef.current === 'popover' ? null : 'popover')
                }}
                onBlur={() => {
                  suppressFocusRef.current = false
                }}
                onPointerMove={() => {
                  suppressFocusRef.current = false
                }}
              >
                {label}
              </button>
            </TooltipTrigger>
          </PopoverTrigger>
          {overlay !== null &&
            (system ? (
              <SubjectOverlays
                overlay={overlay}
                onRestoreFocus={() => {
                  suppressFocusRef.current = true
                }}
              >
                <p>{taskSystemDescription(task.type)}</p>
              </SubjectOverlays>
            ) : (
              <SubjectSession
                overlay={overlay}
                type={task.subject_type as TaskSubjectInfo['subject_type']}
                subjectKey={task.subject_key ?? ''}
                onRestoreFocus={() => {
                  suppressFocusRef.current = true
                }}
              />
            ))}
        </Tooltip>
      </Popover>
    </TooltipProvider>
  )
}

function SubjectOverlays({
  overlay,
  children,
  onRestoreFocus,
}: {
  overlay: Exclude<Overlay, null>
  children: React.ReactNode
  onRestoreFocus: () => void
}) {
  if (overlay === 'tooltip') {
    return <TooltipContent className="max-w-[min(24rem,calc(100vw-2rem))] text-left">{children}</TooltipContent>
  }
  return (
    <PopoverContent className="w-80 max-w-[calc(100vw-2rem)]" onCloseAutoFocus={onRestoreFocus}>
      {children}
    </PopoverContent>
  )
}

// This subscription outlives the tooltip-to-popover switch and ends with the session.
function SubjectSession({
  overlay,
  type,
  subjectKey,
  onRestoreFocus,
}: {
  overlay: Exclude<Overlay, null>
  type: TaskSubjectInfo['subject_type']
  subjectKey: string
  onRestoreFocus: () => void
}) {
  const query = useTaskSubject(type, subjectKey)
  let content: React.ReactNode
  if (!query.data) {
    const message = query.error
      ? query.error instanceof APIError && query.error.status === 404
        ? 'Subject information is no longer available.'
        : 'Unable to load information. Close and reopen to try again.'
      : 'Loading…'
    content = <p role="status">{message}</p>
  } else {
    content = (
      <div className="flex min-w-0 flex-col gap-2">
        {type === 'storage_commit_request' && (
          <CopyableValue label="Registration ID" value={subjectKey} monospace maxLength={24} />
        )}
        <dl className="flex min-w-0 flex-col gap-2">
          {taskSubjectFields(query.data).map((field) => (
            <div key={field.label}>
              <dt className="text-xs text-muted-foreground">{field.label}</dt>
              <dd className="whitespace-pre-wrap break-all text-sm">
                {field.value}
                {field.note && <span className="block text-xs text-muted-foreground">{field.note}</span>}
              </dd>
            </div>
          ))}
        </dl>
        {query.error && (
          <p role="status" className="text-xs text-muted-foreground">
            Unable to update information. Close and reopen to try again.
          </p>
        )}
      </div>
    )
  }
  return (
    <SubjectOverlays overlay={overlay} onRestoreFocus={onRestoreFocus}>
      {content}
    </SubjectOverlays>
  )
}
