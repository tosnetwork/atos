import { createContext, useCallback, useContext, useEffect, useMemo, useState, type PropsWithChildren } from 'react'
import { request } from './api'
import type { Session } from './types'

const KEY = 'atos.tasks.session.v1'
const SCOPES = ['open_tasks:read', 'open_tasks:write', 'open_task_proposals:write', 'capabilities:write', 'jobs:read', 'quotes:read']
type DeviceGrant = { device_code: string; user_code: string; verification_uri_complete: string; expires_in: number; interval: number }
type TokenResponse = { access_token: string; refresh_token: string; expires_in: number; principal_id: string; scopes: string[]; error?: string }
type AuthValue = { session: Session | null; login: () => Promise<void>; logout: () => Promise<void>; loggingIn: boolean; loginCode: string | null }

const AuthContext = createContext<AuthValue | null>(null)
const readSession = () => {
  try { return JSON.parse(sessionStorage.getItem(KEY) || 'null') as Session | null } catch { return null }
}

export function AuthProvider({ children }: PropsWithChildren) {
  const [session, setSession] = useState<Session | null>(readSession)
  const [loggingIn, setLoggingIn] = useState(false)
  const [loginCode, setLoginCode] = useState<string | null>(null)

  const save = useCallback((next: Session | null) => {
    setSession(next)
    if (next) sessionStorage.setItem(KEY, JSON.stringify(next)); else sessionStorage.removeItem(KEY)
  }, [])

  useEffect(() => {
    if (!session) return
    let active = true
    const refresh = () => request<TokenResponse>('/auth/token/refresh', null, { method: 'POST', body: JSON.stringify({ refresh_token: session.refreshToken }) })
      .then(t => { if (active) save({ accessToken: t.access_token, refreshToken: t.refresh_token, expiresAt: Date.now() + t.expires_in * 1000, principalId: t.principal_id, scopes: t.scopes }) })
      .catch(() => { if (active) save(null) })
    const delay = session.expiresAt - Date.now() - 60_000
    if (delay <= 0) refresh()
    else {
      const timer = window.setTimeout(refresh, delay)
      return () => { active = false; clearTimeout(timer) }
    }
    return () => { active = false }
  }, [session, save])

  const login = useCallback(async () => {
    setLoggingIn(true)
    try {
      const grant = await request<DeviceGrant>('/auth/device', null, { method: 'POST', body: JSON.stringify({ client_type: 'web', client_name: 'ATOS Task Marketplace', requested_scopes: SCOPES }) })
      setLoginCode(grant.user_code)
      const popup = window.open(grant.verification_uri_complete, 'atos-consent', 'popup,width=720,height=760')
      if (!popup) throw new Error('Allow pop-ups to continue secure sign in.')
      const deadline = Date.now() + grant.expires_in * 1000
      let interval = Math.max(1, grant.interval) * 1000
      while (Date.now() < deadline) {
        await new Promise(resolve => setTimeout(resolve, interval))
        try {
          const token = await request<TokenResponse>('/auth/device/token', null, { method: 'POST', body: JSON.stringify({ device_code: grant.device_code }) })
          save({ accessToken: token.access_token, refreshToken: token.refresh_token, expiresAt: Date.now() + token.expires_in * 1000, principalId: token.principal_id, scopes: token.scopes })
          popup.close()
          return
        } catch (error) {
          const code = error instanceof Error && 'code' in error ? (error as { code?: string }).code : undefined
          if (code === 'authorization_pending') continue
          if (code === 'slow_down') { interval += 5_000; continue }
          throw error
        }
      }
      throw new Error('The sign-in request expired. Please try again.')
    } finally { setLoggingIn(false); setLoginCode(null) }
  }, [save])

  const logout = useCallback(async () => {
    if (session) await request<void>('/auth/revoke', session, { method: 'POST' }).catch(() => undefined)
    save(null)
  }, [session, save])

  const value = useMemo(() => ({ session, login, logout, loggingIn, loginCode }), [session, login, logout, loggingIn, loginCode])
  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>
}

export function useAuth() {
  const value = useContext(AuthContext)
  if (!value) throw new Error('useAuth must be used inside AuthProvider')
  return value
}
