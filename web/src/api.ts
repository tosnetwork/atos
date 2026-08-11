import type { Acceptance, Job, OpenTask, Proposal, Session } from './types'

const API_ROOT = (import.meta.env.VITE_API_ROOT || '/v1').replace(/\/$/, '')

export class ApiError extends Error {
  constructor(public status: number, message: string, public code?: string) { super(message) }
}

async function parse<T>(response: Response): Promise<T> {
  if (response.ok) return response.status === 204 ? (undefined as T) : response.json()
  const body = await response.json().catch(() => ({})) as { error?: string | { code?: string; message?: string }; error_description?: string; message?: string }
  const nested = typeof body.error === 'object' ? body.error : undefined
  throw new ApiError(response.status, body.error_description || nested?.message || body.message || 'Request failed', typeof body.error === 'string' ? body.error : nested?.code)
}

export async function request<T>(path: string, session?: Session | null, init: RequestInit = {}): Promise<T> {
  const headers = new Headers(init.headers)
  if (init.body) headers.set('Content-Type', 'application/json')
  if (session) headers.set('Authorization', `Bearer ${session.accessToken}`)
  return parse<T>(await fetch(`${API_ROOT}${path}`, { ...init, headers }))
}

export const api = {
  listTasks: (session: Session, limit = 100) => request<{ open_tasks: OpenTask[] }>(`/open-tasks?limit=${limit}`, session),
  getTask: (session: Session, id: string) => request<OpenTask>(`/open-tasks/${encodeURIComponent(id)}`, session),
  getProposals: (session: Session, id: string) => request<{ proposals: Proposal[] }>(`/open-tasks/${encodeURIComponent(id)}/proposals`, session),
  publish: (session: Session, body: object) => request<OpenTask>('/open-tasks', session, { method: 'POST', body: JSON.stringify(body) }),
  accept: (session: Session, taskId: string, proposalId: string, idempotencyKey: string) => request<{ open_task: OpenTask; acceptance: Acceptance }>(`/open-tasks/${encodeURIComponent(taskId)}/proposals/${encodeURIComponent(proposalId)}/accept`, session, { method: 'POST', body: JSON.stringify({ idempotency_key: idempotencyKey }) }),
  getJob: (session: Session, id: string) => request<Job>(`/jobs/${encodeURIComponent(id)}`, session),
}

export function idempotencyKey(action: 'publish' | 'accept') {
  return `task-${action}-${crypto.randomUUID()}`
}
