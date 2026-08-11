import { useEffect, useRef, useState, type FormEvent } from 'react'
import { api, idempotencyKey } from './api'
import { Empty, ErrorNotice } from './components'
import type { Capability, Session } from './types'
import './provider-proposal.css'

type Props = { session: Session; taskId: string; onSubmitted: () => Promise<void> }

export function ProviderProposalForm({ session, taskId, onSubmitted }: Props) {
  const [capabilities, setCapabilities] = useState<Capability[]>([])
  const [loading, setLoading] = useState(true)
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState<unknown>()
  const key = useRef(idempotencyKey('propose'))

  useEffect(() => {
    let active = true
    api.listMyCapabilities(session)
      .then(result => { if (active) setCapabilities(result.capabilities) })
      .catch(caught => { if (active) setError(caught) })
      .finally(() => { if (active) setLoading(false) })
    return () => { active = false }
  }, [session])

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    const form = event.currentTarget
    const data = new FormData(form)
    const message = String(data.get('message') || '').trim()
    const amount = String(data.get('amount') || '').trim()
    setSubmitting(true); setError(undefined)
    try {
      await api.propose(session, taskId, {
        capability_id: String(data.get('capability_id')),
        ...(message ? { message } : {}),
        ...(amount ? { proposed_price: { amount, currency: String(data.get('currency')) } } : {}),
        idempotency_key: key.current,
      })
      key.current = idempotencyKey('propose')
      form.reset()
      await onSubmitted()
    } catch (caught) { setError(caught) } finally { setSubmitting(false) }
  }

  return <section className="proposal-form panel"><p className="eyebrow">Provider application</p><h2>Submit a proposal</h2><p className="muted">Offer one of your capabilities to fulfill this task. ATOS freezes its current version when you submit.</p>
    {error ? <ErrorNotice error={error} /> : null}
    {loading ? <p className="muted">Loading your capabilities…</p> : capabilities.length === 0 ? <Empty>You do not have a capability available for this proposal.</Empty> : <form onSubmit={submit}>
      <label>Capability<select name="capability_id" required defaultValue=""><option value="" disabled>Select your capability</option>{capabilities.map(capability => <option value={capability.id} key={capability.id}>{capability.name} · v{capability.version} · {capability.status}</option>)}</select></label>
      <label>Message <span className="hint">Optional</span><textarea name="message" rows={4} placeholder="Explain your approach and fit…" /></label>
      <div className="row"><label>Reference price <span className="hint">Optional, non-binding</span><input name="amount" inputMode="decimal" pattern="[0-9]+(\.[0-9]+)?" placeholder="4.50" /></label><label>Currency<select name="currency" defaultValue="USD"><option>USD</option><option>EUR</option><option>TOS</option></select></label></div>
      <div className="principle proposal-hint"><span>i</span><p>This price is only a reference hint. The task owner’s final Quote is recalculated by ATOS when a winner is accepted.</p></div>
      <button className="button wide" disabled={submitting}>{submitting ? 'Submitting…' : 'Submit proposal ↗'}</button>
    </form>}
  </section>
}
