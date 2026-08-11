import { useCallback, useEffect, useRef, useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import { api, idempotencyKey } from '../api'
import { DateTime, Empty, ErrorNotice, JsonBlock, Loading, Price, Status } from '../components'
import { useAuth } from '../auth'
import { ProviderProposalForm } from '../ProviderProposalForm'
import type { Acceptance, Job, OpenTask, Proposal, Quote } from '../types'

export function TaskPage() {
  const { taskId = '' } = useParams()
  const { session, login, authorizeProvider, loggingIn, loginCode } = useAuth()
  const [task, setTask] = useState<OpenTask>()
  const [proposals, setProposals] = useState<Proposal[]>([])
  const [acceptance, setAcceptance] = useState<Acceptance>()
  const [quote, setQuote] = useState<Quote>()
  const [job, setJob] = useState<Job>()
  const [error, setError] = useState<unknown>()
  const [loading, setLoading] = useState(false)
  const [accepting, setAccepting] = useState<string>()
  const [withdrawing, setWithdrawing] = useState<string>()
  const keys = useRef(new Map<string, string>())

  const load = useCallback(async () => {
    if (!session) return
    setLoading(true); setError(undefined)
    try { const [nextTask, nextProposals] = await Promise.all([api.getTask(session, taskId), api.getProposals(session, taskId)]); setTask(nextTask); setProposals(nextProposals.proposals) } catch (caught) { setError(caught) } finally { setLoading(false) }
  }, [session, taskId])
  useEffect(() => { load() }, [load])
  useEffect(() => {
    const jobId = acceptance?.job_id || task?.bound_job_id
    if (!session || !jobId) return
    let active = true
    const poll = async () => { try { const result = await api.getJob(session, jobId); if (active) setJob(result) } catch (caught) { if (active) setError(caught) } }
    poll(); const timer = window.setInterval(poll, 5000)
    return () => { active = false; clearInterval(timer) }
  }, [session, acceptance?.job_id, task?.bound_job_id])
  useEffect(() => {
    const quoteId = acceptance?.quote_id || task?.bound_quote_id
    if (!session || !quoteId) return
    let active = true
    // A Quote's economic terms are immutable once created, so unlike Job
    // (which has an evolving state machine worth polling) fetching once is
    // sufficient -- see docs/API.md §3's "returned concrete trust_mode and
    // proof profile are immutable for that Quote."
    api.getQuote(session, quoteId).then(result => { if (active) setQuote(result) }).catch(caught => { if (active) setError(caught) })
    return () => { active = false }
  }, [session, acceptance?.quote_id, task?.bound_quote_id])

  async function accept(proposal: Proposal) {
    if (!session) { await login(); return }
    if (!confirm(`Accept proposal ${proposal.id}? ATOS will create the authoritative Quote and Job.`)) return
    let key = keys.current.get(proposal.id)
    if (!key) { key = idempotencyKey('accept'); keys.current.set(proposal.id, key) }
    setAccepting(proposal.id); setError(undefined)
    try { const result = await api.accept(session, taskId, proposal.id, key); setTask(result.open_task); setAcceptance(result.acceptance); await load() } catch (caught) { setError(caught) } finally { setAccepting(undefined) }
  }

  async function withdraw(proposal: Proposal) {
    if (!session || !confirm(`Withdraw proposal ${proposal.id}?`)) return
    setWithdrawing(proposal.id); setError(undefined)
    try { await api.withdrawProposal(session, taskId, proposal.id); await load() } catch (caught) { setError(caught) } finally { setWithdrawing(undefined) }
  }

  if (!session) return <section className="gate standalone"><p className="eyebrow">Task detail</p><h1>Sign in to view this task</h1><p>ATOS applies viewer-specific server-side redaction after authentication.</p><button className="button" onClick={() => login().catch(setError)}>Sign in securely</button>{error ? <ErrorNotice error={error} /> : null}</section>
  if (loading && !task) return <Loading />
  if (error && !task) return <section className="page"><ErrorNotice error={error} /></section>
  if (!task) return null
  const owner = session.principalId === task.principal_id
  const providerAuthorized = session.scopes.includes('open_task_proposals:write') && session.scopes.includes('capabilities:write')
  const jobId = acceptance?.job_id || task.bound_job_id
  const quoteId = acceptance?.quote_id || task.bound_quote_id
  return <section className="page"><Link to="/tasks" className="back">← All open work</Link><div className="detail-head"><div><div className="title-line"><Status value={task.status} /><code>{task.id}</code></div><h1>{task.title}</h1><p>{task.description || 'No public description provided.'}</p></div><aside><span>Deadline</span><strong><DateTime value={task.expires_at} /></strong><span>Published</span><strong><DateTime value={task.created_at} /></strong><span>Owner</span><code>{task.principal_id}</code></aside></div>
    <div className="detail-grid"><div className="detail-main">{task.input !== undefined && task.input !== null ? <section className="panel"><p className="eyebrow">Committed input</p><JsonBlock value={task.input} /></section> : <div className="notice privacy">Input is private. ATOS returns it only to the owner and, after selection, the winning provider.</div>}<section className="panel"><p className="eyebrow">Requirements</p><dl><div><dt>Trust mode</dt><dd>{task.requested_trust_mode || 'auto'}</dd></div><div><dt>Maximum total</dt><dd><Price money={task.max_total} /></dd></div><div><dt>Proof requirements</dt><dd>{task.proof_requirements ? <JsonBlock value={task.proof_requirements} /> : 'None specified'}</dd></div></dl></section>
      {!owner && task.status === 'open' ? providerAuthorized ? <ProviderProposalForm session={session} taskId={taskId} onSubmitted={load} /> : <section className="proposal-form panel"><p className="eyebrow">Provider application</p><h2>Submit a proposal</h2><p className="muted">Provider tools require explicit permission to manage your capabilities and proposals. Approve with the same ATOS account; the marketplace refuses to switch principals.</p>{error ? <ErrorNotice error={error} /> : null}<button className="button wide" disabled={loggingIn} onClick={() => authorizeProvider().catch(setError)}>{loggingIn ? `Approve ${loginCode || ''}` : 'Enable provider tools'}</button></section> : null}
      {quoteId ? <section className="job-panel"><div><p className="eyebrow">Authoritative quote</p><h2>{quoteId}</h2></div>{quote ? <dl><div><dt>Trust mode</dt><dd>{quote.trust_mode}</dd></div><div><dt>Provider</dt><dd><code>{quote.provider_id}</code></dd></div><div><dt>Capability</dt><dd><code>{quote.capability_id}</code> v{quote.capability_version}</dd></div><div><dt>Max price</dt><dd>{quote.price.currency} {quote.price.total_max}</dd></div><div><dt>Expires</dt><dd><DateTime value={quote.expires_at} /></dd></div></dl> : <p>Loading quote…</p>}</section> : null}
      {jobId ? <section className="job-panel"><div><p className="eyebrow">Generated job</p><h2>{jobId}</h2></div>{job ? <><Status value={job.state} /><dl><div><dt>Trust mode</dt><dd>{job.trust_mode}</dd></div><div><dt>Proof</dt><dd>{(['quote', 'escrow', 'receipt', 'settlement'] as const).map(stage => `${stage}: ${job.proof_status[stage]}`).join(' · ')}</dd></div>{job.failure_reason ? <div><dt>Failure</dt><dd>{job.failure_reason}</dd></div> : null}</dl></> : <p>Loading current Job state…</p>}</section> : acceptance ? <section className="job-panel"><p className="eyebrow">Acceptance operation</p><h2>{acceptance.checkpoint.replaceAll('_', ' ')}</h2><p>ATOS is durably binding the winner to the Quote and Job pipeline.</p></section> : null}</div>
      <section className="proposals"><div className="proposal-head"><div><p className="eyebrow">Proposals</p><h2>{proposals.length} received</h2></div><button className="text-button" onClick={load}>Refresh</button></div>{error ? <ErrorNotice error={error} /> : null}{proposals.length === 0 ? <Empty>No proposals yet.</Empty> : proposals.map(proposal => <article className="proposal" key={proposal.id}><div className="proposal-title"><Status value={proposal.status} /><code>{proposal.id}</code></div><h3>{proposal.capability_id}</h3><p className="version">Version {proposal.capability_version}</p>{proposal.message ? <blockquote>{proposal.message}</blockquote> : <p className="muted">Proposal details are private for this viewer.</p>}<div className="proposal-price"><span>Reference price · non-binding</span><strong><Price money={proposal.proposed_price} /></strong></div>{owner && task.status === 'open' && proposal.status === 'submitted' ? <button className="button wide" disabled={!!accepting} onClick={() => accept(proposal)}>{accepting === proposal.id ? 'Binding winner…' : 'Accept & create Job'}</button> : null}{session.principalId === proposal.provider_id && proposal.status === 'submitted' ? <button className="withdraw-button wide" disabled={!!withdrawing} onClick={() => withdraw(proposal)}>{withdrawing === proposal.id ? 'Withdrawing…' : 'Withdraw proposal'}</button> : null}</article>)}</section></div>
  </section>
}
