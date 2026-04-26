import { createFileRoute, Outlet } from '@tanstack/react-router'

export const Route = createFileRoute('/buckets')({
  component: BucketsLayout,
})

function BucketsLayout() {
  return <Outlet />
}
