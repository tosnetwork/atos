import { Link, Navigate, Route, Routes } from 'react-router-dom'
import { useAuth } from './auth'
import { BrowsePage } from './pages/BrowsePage'
import { NewTaskPage } from './pages/NewTaskPage'
import { TaskPage } from './pages/TaskPage'

function Header() {
  const { session, login, logout, loggingIn, loginCode } = useAuth()
  return <header><Link className="brand" to="/tasks"><span className="mark">A</span><span>ATOS</span></Link><nav><Link to="/tasks">Explore</Link><Link to="/tasks/new">Post a task</Link></nav><div className="account">{session ? <><span className="principal">{session.principalId}</span><button className="text-button" onClick={logout}>Sign out</button></> : <button className="button small" disabled={loggingIn} onClick={() => login().catch(error => alert(error.message))}>{loggingIn ? `Approve ${loginCode || ''}` : 'Sign in'}</button>}</div></header>
}

export function App() {
  return <><Header /><main><Routes><Route path="/tasks" element={<BrowsePage />} /><Route path="/tasks/new" element={<NewTaskPage />} /><Route path="/tasks/:taskId" element={<TaskPage />} /><Route path="*" element={<Navigate to="/tasks" replace />} /></Routes></main><footer><span>ATOS</span><span>One market. Verifiable work.</span></footer></>
}
