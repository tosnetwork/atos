import { useMemo, useRef, useState, type FormEvent } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import { api, idempotencyKey } from '../api'
import { ErrorNotice } from '../components'
import { useAuth } from '../auth'

const jsonObject = (value: string, label: string) => {
  const parsed: unknown = JSON.parse(value || '{}')
  if (!parsed || Array.isArray(parsed) || typeof parsed !== 'object') throw new Error(`${label} must be a JSON object.`)
  return parsed as Record<string, unknown>
}

export function NewTaskPage() {
  const { session, login } = useAuth()
  const navigate = useNavigate()
  const key = useRef(idempotencyKey('publish'))
  const [error, setError] = useState<unknown>()
  const [submitting, setSubmitting] = useState(false)
  const [trust, setTrust] = useState('managed')
  // datetime-local's value/min/max are timezone-naive local wall-clock
  // strings (HTML spec) -- toISOString() would emit UTC, which silently
  // shifts the minimum by the browser's UTC offset for anyone not in UTC+0.
  const minimum = useMemo(() => {
    const d = new Date(Date.now() + 60_000)
    d.setSeconds(0, 0)
    const pad = (n: number) => String(n).padStart(2, '0')
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`
  }, [])

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    if (!session) { await login(); return }
    const data = new FormData(event.currentTarget)
    setSubmitting(true); setError(undefined)
    try {
      const amount = String(data.get('amount') || '').trim()
      const task = await api.publish(session, {
        title: data.get('title'), description: data.get('description'), input: jsonObject(String(data.get('input')), 'Input'),
        requested_trust_mode: trust,
        proof_requirements: { network_verifiable_receipt: data.get('network_receipt') === 'on' },
        constraints: { ...(amount ? { max_total: { amount, currency: data.get('currency') } } : {}) },
        expires_at: new Date(String(data.get('expires_at'))).toISOString(), idempotency_key: key.current,
      })
      navigate(`/tasks/${task.id}`, { replace: true })
    } catch (caught) { setError(caught) } finally { setSubmitting(false) }
  }

  return <section className="form-shell"><div className="form-intro"><Link to="/tasks" className="back">← Marketplace</Link><p className="eyebrow">Publish demand</p><h1>Describe the outcome,<br /><em>not the provider.</em></h1><p>Providers will propose a capability. Your input, trust requirements, deadline, and maximum budget stay fixed when you publish.</p><div className="principle"><span>i</span><p>A proposal price is only a hint. ATOS calculates the binding Quote when you accept a winner.</p></div></div>
    <form className="task-form" onSubmit={submit}><div className="form-section"><span className="step">01</span><div><h2>The ask</h2><p>What outcome do you need?</p></div></div><label>Title<input name="title" required maxLength={160} placeholder="Summarize Q3 filings" /></label><label>Description<textarea name="description" rows={5} placeholder="Define a clear, testable outcome…" /></label><label>Input <span className="hint">JSON object — only you and the winning provider can see it</span><textarea className="mono" name="input" rows={7} defaultValue={'{\n  "document_url": ""\n}'} required /></label>
      <div className="form-section"><span className="step">02</span><div><h2>Commercial bounds</h2><p>Set the ceiling, never the final price.</p></div></div><div className="row"><label>Maximum total<input name="amount" inputMode="decimal" pattern="[0-9]+(\.[0-9]+)?" placeholder="5.00" /></label><label>Currency<select name="currency" defaultValue="USD"><option>USD</option><option>EUR</option><option>TOS</option></select></label></div><label>Expires at <span className="required">Required</span><input name="expires_at" type="datetime-local" min={minimum} required /></label>
      <div className="form-section"><span className="step">03</span><div><h2>Trust</h2><p>Choose the execution assurance you require.</p></div></div><div className="trust-options">{['auto','managed','verified','native'].map(value => <button className={trust === value ? 'selected' : ''} type="button" key={value} onClick={() => setTrust(value)}><strong>{value}</strong><span>{value === 'auto' ? 'Let policy resolve' : `${value} execution`}</span></button>)}</div><label className="check"><input name="network_receipt" type="checkbox" /><span><strong>Network-verifiable receipt</strong><small>Require proof that can be verified outside ATOS.</small></span></label>
      {error ? <ErrorNotice error={error} /> : null}<div className="submit-row"><p>Publishing creates a durable OpenTask.</p><button className="button" disabled={submitting}>{submitting ? 'Publishing…' : session ? 'Publish task ↗' : 'Sign in to publish'}</button></div></form></section>
}
