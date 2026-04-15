import { Outlet, createFileRoute } from '@tanstack/react-router'

export const Route = createFileRoute('/buckets')({
  component: BucketsLayout,
})

function BucketsLayout() {
  return <Outlet />
}
