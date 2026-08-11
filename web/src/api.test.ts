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

  it('submits only the frozen proposal request fields with idempotency in the body', async () => {
    const fetch = vi.fn().mockResolvedValue(new Response(JSON.stringify({ id: 'prop_1' }), { status: 201, headers: { 'Content-Type': 'application/json' } }))
    vi.stubGlobal('fetch', fetch)
    await api.propose(session, 'otask_1', { capability_id: 'cap_1', message: 'A good fit', proposed_price: { amount: '4.50', currency: 'USD' }, idempotency_key: 'task-propose-fixed' })
    const [url, init] = fetch.mock.calls[0]
    expect(url).toBe('/v1/open-tasks/otask_1/proposals')
    expect(init.method).toBe('POST')
    expect(JSON.parse(init.body)).toEqual({ capability_id: 'cap_1', message: 'A good fit', proposed_price: { amount: '4.50', currency: 'USD' }, idempotency_key: 'task-propose-fixed' })
    expect(JSON.parse(init.body)).not.toHaveProperty('capability_version')
    expect((init.headers as Headers).has('Idempotency-Key')).toBe(false)
  })

  it('withdraws through the proposal action endpoint without a body', async () => {
    const fetch = vi.fn().mockResolvedValue(new Response(JSON.stringify({ id: 'prop_1', status: 'withdrawn' }), { status: 200, headers: { 'Content-Type': 'application/json' } }))
    vi.stubGlobal('fetch', fetch)
    await api.withdrawProposal(session, 'otask/one', 'prop/two')
    const [url, init] = fetch.mock.calls[0]
    expect(url).toBe('/v1/open-tasks/otask%2Fone/proposals/prop%2Ftwo/withdraw')
    expect(init.method).toBe('POST')
    expect(init.body).toBeUndefined()
    expect((init.headers as Headers).has('Idempotency-Key')).toBe(false)
  })

  it('generates a distinct action-scoped idempotency key', () => {
    const one = idempotencyKey('publish')
    const two = idempotencyKey('publish')
    expect(one).toMatch(/^task-publish-/)
    expect(one).not.toBe(two)
  })

  it('generates proposal-scoped idempotency keys', () => {
    expect(idempotencyKey('propose')).toMatch(/^task-propose-/)
  })
})
