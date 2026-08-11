import { afterEach, describe, expect, it, vi } from 'vitest'
import { api, idempotencyKey } from './api'
import type { Session } from './types'

const session: Session = { accessToken: 'access-token', refreshToken: 'refresh-token', expiresAt: Date.now() + 60_000, principalId: 'prn_owner', scopes: ['open_tasks:read', 'open_tasks:write', 'jobs:read'] }

afterEach(() => vi.unstubAllGlobals())

describe('frozen OpenTask REST contract', () => {
  it('browses the public endpoint without mine=true', async () => {
    const fetch = vi.fn().mockResolvedValue(new Response(JSON.stringify({ open_tasks: [] }), { status: 200, headers: { 'Content-Type': 'application/json' } }))
    vi.stubGlobal('fetch', fetch)
    await api.listTasks(session)
    expect(fetch).toHaveBeenCalledOnce()
    const [url, init] = fetch.mock.calls[0]
    expect(url).toBe('/v1/open-tasks?limit=100')
    expect(url).not.toContain('mine=true')
    expect((init.headers as Headers).get('Authorization')).toBe('Bearer access-token')
  })

  it('puts accept idempotency in the JSON body, not a header', async () => {
    const fetch = vi.fn().mockResolvedValue(new Response(JSON.stringify({ open_task: {}, acceptance: {} }), { status: 200, headers: { 'Content-Type': 'application/json' } }))
    vi.stubGlobal('fetch', fetch)
    await api.accept(session, 'otask_1', 'prop_1', 'task-accept-fixed')
    const [, init] = fetch.mock.calls[0]
    expect(JSON.parse(init.body)).toEqual({ idempotency_key: 'task-accept-fixed' })
    expect((init.headers as Headers).has('Idempotency-Key')).toBe(false)
  })

  it('generates a distinct action-scoped idempotency key', () => {
    const one = idempotencyKey('publish')
    const two = idempotencyKey('publish')
    expect(one).toMatch(/^task-publish-/)
    expect(one).not.toBe(two)
  })
})
