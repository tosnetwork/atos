from pathlib import Path

p = Path("internal/service/economic_recovery.go")
text = p.read_text()
anchor = '''func (s *JobService) ReconcileJob(ctx context.Context, jobID string) (domain.Job, error) {\n'''
helper = '''func (s *JobService) reconcilePrepareAndRun(ctx context.Context, jobID string) (domain.Job, error) {\n\tlock := s.jobLock(jobID)\n\tlock.Lock()\n\tjob, _, err := s.prepareExecutionUnderLock(ctx, jobID)\n\tlock.Unlock()\n\tif err != nil {\n\t\tif current, getErr := s.store.GetJob(ctx, jobID); getErr == nil {\n\t\t\treturn current, err\n\t\t}\n\t\treturn job, err\n\t}\n\tif job.State == domain.JobWorking && job.EconomicState == domain.EconomicEscrowReserved {\n\t\treturn s.recoverProviderExecution(ctx, jobID, true)\n\t}\n\treturn job, nil\n}\n\n'''
if text.count(anchor) != 1:
    raise SystemExit("ReconcileJob anchor not found")
text = text.replace(anchor, helper + anchor, 1)
old = '''\tcase domain.EconomicNone:\n\t\tresult, execErr := s.executeJob(ctx, jobID, false, 0)\n\t\treturn result.Job, execErr\n'''
new = '''\tcase domain.EconomicNone:\n\t\treturn s.reconcilePrepareAndRun(ctx, jobID)\n'''
if text.count(old) != 1:
    raise SystemExit("EconomicNone reconcile block not found")
text = text.replace(old, new, 1)
old = '''\t\tresult, execErr := s.executeJob(ctx, jobID, false, 0)\n\t\treturn result.Job, execErr\n\tcase domain.EconomicEscrowPending:\n'''
new = '''\t\treturn s.reconcilePrepareAndRun(ctx, jobID)\n\tcase domain.EconomicEscrowPending:\n'''
if text.count(old) != 1:
    raise SystemExit("EconomicDebited reconcile block not found")
text = text.replace(old, new, 1)
old = '''\t\tresult, execErr := s.executeJob(ctx, jobID, false, 0)\n\t\treturn result.Job, execErr\n\tcase domain.EconomicEscrowReserved:\n'''
new = '''\t\treturn s.reconcilePrepareAndRun(ctx, jobID)\n\tcase domain.EconomicEscrowReserved:\n'''
if text.count(old) != 1:
    raise SystemExit("EconomicEscrowPending reconcile block not found")
text = text.replace(old, new, 1)
p.write_text(text)
print("stage2 reconciliation now converges in one pass")
