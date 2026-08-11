import { describe, expect, it, vi } from 'vitest'
import { prepareAuthorizedSession, revokeSessionDevice, sessionFromToken } from './auth'
import type { Session } from './types'

const token = {
  access_token: 'provider-access', refresh_token: 'provider-refresh', expires_in: 3600,
  principal_id: 'prn_current', device_id: 'dev_provider', scopes: ['open_tasks:read', 'open_task_proposals:write', 'capabilities:write'],
}

describe('provider scope authorization identity continuity', () => {
  it('accepts a grant for the current principal', () => {
    const session = sessionFromToken(token, 'prn_current')
    expect(session.principalId).toBe('prn_current')
    expect(session.accessToken).toBe('provider-access')
    expect(session.deviceId).toBe('dev_provider')
  })

  it('refuses to replace the session with a different principal', () => {
    expect(() => sessionFromToken({ ...token, principal_id: 'prn_other' }, 'prn_current'))
      .toThrow(/different ATOS account/)
  })

  it('revokes a complete device and propagates cleanup failures', async () => {
    const session = sessionFromToken(token)
    const send = vi.fn().mockRejectedValue(new Error('network unavailable'))
    await expect(revokeSessionDevice(session, send)).rejects.toThrow('network unavailable')
    expect(send).toHaveBeenCalledWith('/auth/devices/dev_provider', session, { method: 'DELETE' })
  })

  it('uses token revocation for legacy sessions without a device id', async () => {
    const session: Session = { ...sessionFromToken(token), deviceId: undefined }
    const send = vi.fn().mockResolvedValue(undefined)
    await revokeSessionDevice(session, send)
    expect(send).toHaveBeenCalledWith('/auth/revoke', session, { method: 'POST' })
  })

  it('revokes a mismatched grant and keeps it from becoming a session', async () => {
    const current = sessionFromToken(token)
    const revoke = vi.fn().mockResolvedValue(undefined)
    const mismatched = { ...token, principal_id: 'prn_other', device_id: 'dev_other' }
    await expect(prepareAuthorizedSession(mismatched, current, revoke)).rejects.toThrow(/different ATOS account/)
    expect(revoke).toHaveBeenCalledOnce()
    expect(revoke.mock.calls[0][0]).toMatchObject({ principalId: 'prn_other', deviceId: 'dev_other' })
  })

  it('retires the old device before completing a same-principal upgrade', async () => {
    const current = { ...sessionFromToken(token), accessToken: 'old-access', deviceId: 'dev_old' }
    const revoke = vi.fn().mockResolvedValue(undefined)
    const upgraded = await prepareAuthorizedSession(token, current, revoke)
    expect(revoke).toHaveBeenCalledOnce()
    expect(revoke).toHaveBeenCalledWith(current)
    expect(upgraded.deviceId).toBe('dev_provider')
  })

  it('aborts an upgrade and revokes the new grant when retiring the old device fails', async () => {
    const current = { ...sessionFromToken(token), accessToken: 'old-access', deviceId: 'dev_old' }
    const revoke = vi.fn()
      .mockRejectedValueOnce(new Error('old revoke failed'))
      .mockResolvedValueOnce(undefined)
    await expect(prepareAuthorizedSession(token, current, revoke)).rejects.toThrow(/previous device could not be revoked/)
    expect(revoke).toHaveBeenCalledTimes(2)
    expect(revoke.mock.calls[1][0]).toMatchObject({ accessToken: 'provider-access', deviceId: 'dev_provider' })
  })
})
