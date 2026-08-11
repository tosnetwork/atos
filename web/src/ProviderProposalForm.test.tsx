// @vitest-environment jsdom
import '@testing-library/jest-dom/vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { api } from './api'
import { ProviderProposalForm } from './ProviderProposalForm'
import type { Capability, Session } from './types'

const session: Session = { accessToken: 'token', refreshToken: 'refresh', expiresAt: Date.now() + 60_000, principalId: 'provider_1', scopes: ['open_task_proposals:write', 'capabilities:write'] }
const capability = (id: string, status: string): Capability => ({ id, status, provider_id: 'provider_1', name: `Capability ${id}`, description: '', version: '1.2.0' })

afterEach(() => vi.restoreAllMocks())

describe('ProviderProposalForm', () => {
  it('offers only active capabilities and explains when none are active', async () => {
    vi.spyOn(api, 'listMyCapabilities').mockResolvedValue({ capabilities: [capability('cap_paused', 'paused')] })
    render(<ProviderProposalForm session={session} taskId="otask_1" onSubmitted={vi.fn()} />)
    expect(await screen.findByText('You do not have an active capability available for this proposal.')).toBeInTheDocument()
    expect(screen.queryByRole('option', { name: /cap_paused/ })).not.toBeInTheDocument()
  })

  it('submits the selected active capability without a capability version', async () => {
    const user = userEvent.setup()
    vi.spyOn(api, 'listMyCapabilities').mockResolvedValue({ capabilities: [capability('cap_active', 'active'), capability('cap_paused', 'paused')] })
    const propose = vi.spyOn(api, 'propose').mockResolvedValue({ id: 'prop_1' } as never)
    const onSubmitted = vi.fn().mockResolvedValue(undefined)
    render(<ProviderProposalForm session={session} taskId="otask_1" onSubmitted={onSubmitted} />)

    await user.selectOptions(await screen.findByLabelText('Capability'), 'cap_active')
    await user.type(screen.getByLabelText(/Message/), 'Ready to deliver')
    await user.type(screen.getByLabelText(/Reference price/), '4.50')
    await user.click(screen.getByRole('button', { name: 'Submit proposal ↗' }))

    await waitFor(() => expect(propose).toHaveBeenCalledOnce())
    const body = propose.mock.calls[0][2]
    expect(body).toMatchObject({ capability_id: 'cap_active', message: 'Ready to deliver', proposed_price: { amount: '4.50', currency: 'USD' } })
    expect(body).not.toHaveProperty('capability_version')
    expect(body.idempotency_key).toMatch(/^task-propose-/)
    expect(onSubmitted).toHaveBeenCalledOnce()
    expect(screen.queryByRole('option', { name: /cap_paused/ })).not.toBeInTheDocument()
  })
})
