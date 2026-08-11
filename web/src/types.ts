export type Money = { amount: string; currency: string }
export type TaskStatus = 'open' | 'accepted' | 'fulfilled' | 'cancelled' | 'expired'

export type OpenTask = {
  id: string
  principal_id: string
  title: string
  description?: string
  input?: Record<string, unknown> | null
  requested_trust_mode?: 'auto' | 'managed' | 'verified' | 'native'
  proof_requirements?: Record<string, unknown>
  max_total?: Money
  expires_at: string
  status: TaskStatus
  accepted_proposal_id?: string
  bound_quote_id?: string
  bound_job_id?: string
  created_at: string
  updated_at?: string
}

export type Proposal = {
  id: string
  task_id: string
  provider_id: string
  capability_id: string
  capability_version: string
  message?: string
  proposed_price?: Money
  status: 'submitted' | 'withdrawn' | 'accepted' | 'rejected'
  withdrawn_at?: string
  created_at: string
  updated_at: string
}

export type Acceptance = {
  id: string
  task_id: string
  proposal_id: string
  checkpoint: 'intent_persisted' | 'winner_claimed' | 'quote_binding_pending' | 'quote_bound' | 'job_binding_pending' | 'job_bound' | 'completed' | 'failed' | 'reconciling'
  quote_id?: string
  job_id?: string
}

export type Job = { job_id: string; state: string; trust_mode: string; proof_status: string; failure_reason?: string; output?: Record<string, unknown>; created_at: string; completed_at?: string }

export type Session = { accessToken: string; refreshToken: string; expiresAt: number; principalId: string; scopes: string[] }
