import { describe, expect, it } from 'vitest'
import { sessionFromToken } from './auth'

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
})
