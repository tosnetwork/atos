import type { ReactNode } from 'react'
import type { Money, TaskStatus } from './types'

export function Status({ value }: { value: TaskStatus | string }) { return <span className={`status status-${value}`}>{value.replaceAll('_', ' ')}</span> }
export function Price({ money }: { money?: Money }) { return money ? <span>{money.currency} {money.amount}</span> : <span className="muted">Not specified</span> }
export function DateTime({ value }: { value: string }) { return <time dateTime={value}>{new Intl.DateTimeFormat(undefined, { dateStyle: 'medium', timeStyle: 'short' }).format(new Date(value))}</time> }
export function Empty({ children }: { children: ReactNode }) { return <div className="empty"><span>○</span><p>{children}</p></div> }
export function Loading() { return <div className="loading"><i /><i /><i /><span>Loading marketplace</span></div> }
export function ErrorNotice({ error }: { error: unknown }) { return <div className="notice error">{error instanceof Error ? error.message : 'Something went wrong.'}</div> }
export function JsonBlock({ value }: { value: unknown }) { return <pre className="json">{JSON.stringify(value, null, 2)}</pre> }
