import { Gauge, Loader2 } from 'lucide-react'
import { useId } from 'react'
import type { ProviderUploadSpeedEligibility, ProviderUploadSpeedTest } from '@/api/client'
import { Button } from '@/components/ui/button'
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'
import { canTestProviderUploadSpeed, providerUploadSpeedUnavailableReason } from '@/lib/provider-upload-speed'

export function ProviderUploadSpeedButton({
  provider,
  name,
  starting = false,
  compact = false,
  onTest,
}: {
  provider?: ProviderUploadSpeedEligibility & { upload_speed_test?: ProviderUploadSpeedTest }
  name: string
  starting?: boolean
  compact?: boolean
  onTest: () => void
}) {
  const reasonID = useId()
  const testing = starting || provider?.upload_speed_test?.state === 'testing'
  const disabled = testing || !canTestProviderUploadSpeed(provider)
  const reason = testing ? 'A test is already running.' : providerUploadSpeedUnavailableReason(provider)
  const button = (
    <Button
      type="button"
      size={compact ? 'icon-xs' : 'sm'}
      variant={compact ? 'ghost' : 'outline'}
      disabled={disabled}
      aria-label={compact ? `${testing ? 'Testing' : 'Test'} upload speed for ${name}` : undefined}
      aria-describedby={disabled && !compact ? reasonID : undefined}
      onClick={onTest}
    >
      {testing ? (
        <Loader2 data-icon={compact ? undefined : 'inline-start'} className="animate-spin" />
      ) : (
        <Gauge data-icon={compact ? undefined : 'inline-start'} />
      )}
      {!compact && (testing ? 'Testing upload speed…' : 'Test upload speed')}
    </Button>
  )
  return (
    <div className="inline-flex flex-col gap-1">
      {compact ? (
        <Tooltip>
          <TooltipTrigger asChild>
            <fieldset
              tabIndex={disabled ? 0 : undefined}
              aria-label={disabled ? `Upload speed test unavailable for ${name}` : undefined}
              className="inline-flex"
              onClick={(event) => event.stopPropagation()}
              onKeyDown={(event) => {
                if (event.key === 'Enter' || event.key === ' ') event.stopPropagation()
              }}
            >
              {button}
            </fieldset>
          </TooltipTrigger>
          <TooltipContent>{reason ?? 'Test upload speed with 32 MiB'}</TooltipContent>
        </Tooltip>
      ) : (
        button
      )}
      {!compact && disabled && (
        <span id={reasonID} className="text-xs text-muted-foreground">
          {reason}
        </span>
      )}
    </div>
  )
}
