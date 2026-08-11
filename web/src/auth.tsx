import { createContext, useCallback, useContext, useEffect, useMemo, useState, type PropsWithChildren } from 'react'
import { request } from './api'
import type { Session } from './types'

const KEY = 'atos.tasks.session.v1'
const SCOPES = ['open_tasks:read', 'open_tasks:write', 'jobs:read', 'quotes:read']
const PROVIDER_SCOPES = [...SCOPES, 'open_task_proposals:write', 'capabilities:write']
type DeviceGrant = { device_code: string; user_code: string; verification_uri_complete: string; expires_in: number; interval: number }
type TokenResponse = { access_token: string; refresh_token: string; expires_in: number; principal_id: string; device_id?: string; scopes: string[]; error?: string }
type AuthValue = { session: Session | null; login: () => Promise<void>; authorizeProvider: () => Promise<void>; logout: () => Promise<void>; loggingIn: boolean; loginCode: string | null }

const AuthContext = createContext<AuthValue | null>(null)
const readSession = () => {
  try { return JSON.parse(sessionStorage.getItem(KEY) || 'null') as Session | null } catch { return null }
}

export function sessionFromToken(token: TokenResponse, expectedPrincipalId?: string): Session {
  if (expectedPrincipalId && token.principal_id !== expectedPrincipalId) {
    throw new Error('Provider authorization used a different ATOS account. Your existing session was kept; approve again with the same account.')
  }
  return { accessToken: token.access_token, refreshToken: token.refresh_token, expiresAt: Date.now() + token.expires_in * 1000, principalId: token.principal_id, deviceId: token.device_id, scopes: token.scopes }
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
      .then(t => { if (active) save(sessionFromToken(t, session.principalId)) })
      .catch(() => { if (active) save(null) })
    const delay = session.expiresAt - Date.now() - 60_000
    if (delay <= 0) refresh()
    else {
      const timer = window.setTimeout(refresh, delay)
      return () => { active = false; clearTimeout(timer) }
    }
    return () => { active = false }
  }, [session, save])

  const authorize = useCallback(async (requestedScopes: string[], clientName: string, expectedPrincipalId?: string) => {
    setLoggingIn(true)
    try {
      const grant = await request<DeviceGrant>('/auth/device', null, { method: 'POST', body: JSON.stringify({ client_type: 'web', client_name: clientName, requested_scopes: requestedScopes }) })
      setLoginCode(grant.user_code)
      const popup = window.open(grant.verification_uri_complete, 'atos-consent', 'popup,width=720,height=760')
      if (!popup) throw new Error('Allow pop-ups to continue secure sign in.')
      const deadline = Date.now() + grant.expires_in * 1000
      let interval = Math.max(1, grant.interval) * 1000
      while (Date.now() < deadline) {
        await new Promise(resolve => setTimeout(resolve, interval))
        try {
          const token = await request<TokenResponse>('/auth/device/token', null, { method: 'POST', body: JSON.stringify({ device_code: grant.device_code }) })
          let nextSession: Session
          try {
            nextSession = sessionFromToken(token, expectedPrincipalId)
          } catch (identityError) {
            const mismatchedSession = sessionFromToken(token)
            if (mismatchedSession.deviceId) await request<void>(`/auth/devices/${encodeURIComponent(mismatchedSession.deviceId)}`, mismatchedSession, { method: 'DELETE' }).catch(() => undefined)
            else await request<void>('/auth/revoke', mismatchedSession, { method: 'POST' }).catch(() => undefined)
            popup.close()
            throw identityError
          }
          save(nextSession)
          if (expectedPrincipalId && session) {
            if (session.deviceId) await request<void>(`/auth/devices/${encodeURIComponent(session.deviceId)}`, session, { method: 'DELETE' }).catch(() => undefined)
            else await request<void>('/auth/revoke', session, { method: 'POST' }).catch(() => undefined)
          }
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
  }, [save, session])

  const login = useCallback(() => authorize(SCOPES, 'ATOS Task Marketplace'), [authorize])
  const authorizeProvider = useCallback(() => {
    if (!session) return Promise.reject(new Error('Sign in before enabling provider tools.'))
    return authorize(PROVIDER_SCOPES, 'ATOS Provider Tools', session.principalId)
  }, [authorize, session])

  const logout = useCallback(async () => {
    if (session) await request<void>('/auth/revoke', session, { method: 'POST' }).catch(() => undefined)
    save(null)
  }, [session, save])

  const value = useMemo(() => ({ session, login, authorizeProvider, logout, loggingIn, loginCode }), [session, login, authorizeProvider, logout, loggingIn, loginCode])
  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>
}

export function useAuth() {
  const value = useContext(AuthContext)
  if (!value) throw new Error('useAuth must be used inside AuthProvider')
  return value
}
