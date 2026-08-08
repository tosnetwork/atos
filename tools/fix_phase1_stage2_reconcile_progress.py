from pathlib import Path

p = Path("internal/service/economic_recovery.go")
text = p.read_text()
anchor = 'func (s *JobService) ReconcileJob(ctx context.Context, jobID string) (domain.Job, error) {\n'
helper = '''func (s *JobService) reconcilePrepareAndRun(ctx context.Context, jobID string) (domain.Job, error) {
    lock := s.jobLock(jobID)
    lock.Lock()
    job, _, err := s.prepareExecutionUnderLock(ctx, jobID)
    lock.Unlock()
    if err != nil {
        if current, getErr := s.store.GetJob(ctx, jobID); getErr == nil {
            return current, err
        }
        return job, err
    }
    if job.State == domain.JobWorking && job.EconomicState == domain.EconomicEscrowReserved {
        return s.recoverProviderExecution(ctx, jobID, true)
    }
    return job, nil
}

'''
if text.count(anchor) != 1:
    raise SystemExit("ReconcileJob anchor not found")
text = text.replace(anchor, helper + anchor, 1)
replacements = [
    (
        '''    case domain.EconomicNone:
        result, execErr := s.executeJob(ctx, jobID, false, 0)
        return result.Job, execErr
''',
        '''    case domain.EconomicNone:
        return s.reconcilePrepareAndRun(ctx, jobID)
''',
        "EconomicNone",
    ),
    (
        '''        result, execErr := s.executeJob(ctx, jobID, false, 0)
        return result.Job, execErr
    case domain.EconomicEscrowPending:
''',
        '''        return s.reconcilePrepareAndRun(ctx, jobID)
    case domain.EconomicEscrowPending:
''',
        "EconomicDebited",
    ),
    (
        '''        result, execErr := s.executeJob(ctx, jobID, false, 0)
        return result.Job, execErr
    case domain.EconomicEscrowReserved:
''',
        '''        return s.reconcilePrepareAndRun(ctx, jobID)
    case domain.EconomicEscrowReserved:
''',
        "EconomicEscrowPending",
    ),
]
for old, new, label in replacements:
    if text.count(old) != 1:
        raise SystemExit(f"{label} reconcile block not found")
    text = text.replace(old, new, 1)
p.write_text(text)
print("stage2 reconciliation now converges in one pass")
