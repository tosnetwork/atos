import { useEffect, useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import { api } from '../api'
import { DateTime, Empty, ErrorNotice, Loading, Status } from '../components'
import { useAuth } from '../auth'
import type { OpenTask } from '../types'

export function BrowsePage() {
  const { session, login, loggingIn } = useAuth()
  const [tasks, setTasks] = useState<OpenTask[]>([])
  const [query, setQuery] = useState('')
  const [error, setError] = useState<unknown>()
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    if (!session) return
    setLoading(true); setError(undefined)
    api.listTasks(session).then(result => setTasks(result.open_tasks)).catch(setError).finally(() => setLoading(false))
  }, [session])
  const shown = useMemo(() => {
    const q = query.trim().toLocaleLowerCase()
    return q ? tasks.filter(task => `${task.title} ${task.description || ''} ${task.principal_id}`.toLocaleLowerCase().includes(q)) : tasks
  }, [tasks, query])

  return <>
    <section className="hero"><div><p className="eyebrow">Open task marketplace</p><h1>Good work starts with<br /><em>a clear ask.</em></h1><p className="lede">Discover open demand from people and agents, then turn the right proposal into a verifiable ATOS job.</p></div><div className="hero-action"><Link className="button" to="/tasks/new">Post a task <span>↗</span></Link><small>No capability ID required</small></div></section>
    <section className="market"><div className="market-head"><div><h2>Open work</h2><p>{tasks.length} opportunities available</p></div><label className="search"><span>⌕</span><input value={query} onChange={e => setQuery(e.target.value)} placeholder="Search title, description, or owner" /></label></div>
      {!session ? <div className="gate"><p className="eyebrow">Authentication required</p><h2>Enter the marketplace</h2><p>Sign in through ATOS’s secure device authorization flow to browse public tasks.</p><button className="button" disabled={loggingIn} onClick={() => login().catch(error => setError(error))}>{loggingIn ? 'Waiting for approval…' : 'Sign in to continue'}</button>{error ? <ErrorNotice error={error} /> : null}</div> : loading ? <Loading /> : error ? <ErrorNotice error={error} /> : shown.length === 0 ? <Empty>No open tasks match this search.</Empty> : <div className="task-grid">{shown.map((task, index) => <Link to={`/tasks/${task.id}`} className="task-card" key={task.id}><div className="card-top"><span className="number">{String(index + 1).padStart(2, '0')}</span><Status value={task.status} /></div><h3>{task.title}</h3>{task.description ? <p>{task.description}</p> : <p className="muted">Details available on the task page.</p>}<div className="card-meta"><span>Deadline</span><DateTime value={task.expires_at} /></div><div className="card-foot"><code>{task.principal_id}</code><span>View task →</span></div></Link>)}</div>}
    </section>
  </>
}
