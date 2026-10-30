# CE-06 — Demonstration Guide (live, ~15 min)

Run everything from WSL (`wsl -d Ubuntu -u root`) unless noted. Windows browser
links work as-is (`localhost` ports are forwarded).

## 0. Start the cluster (30 s)

```bash
bash scripts/start_services.sh
```
Show: 5 daemons (NameNode, DataNode, ResourceManager, NodeManager,
JobHistoryServer). Open in browser:
- `http://localhost:8088` → Nodes tab: 1 RUNNING NodeManager (2048 MB, 4 vCores)
- `http://localhost:9870` → Datanodes: 1 live, 355.8 MB under `/data/clean`

**Talking point:** all daemons are `setsid --fork`-detached so they survive the
shell closing; heaps are 256 m to fit an 8 GB laptop; replication=1 (single DN).

## 1. Dataset (30 s)

```bash
hdfs dfs -ls -h /data/clean /data/malformed
head -2 dataset/malformed/nasa_access_malformed.log   # shows CE06_BAD_RECORD marker
```
**Talking point:** 3,461,565 clean + 48 naturally malformed + 20 injected bad
records (seed=42, marker `CE06_BAD_RECORD`, ~every 173k lines).

## 2. Exp 1 — Baseline job (~2.5 min)

```bash
bash scripts/run_experiment.sh 01 /results/exp01_baseline NORMAL \
    /data/clean/nasa_access_clean.log
```
Show while it runs: `http://localhost:8088` (application RUNNING → 12 maps) and
afterwards `http://localhost:19888/jobhistory/job/job_…_0001` → Tasks tab.
**Show:** 13 tasks, each with **1 SUCCEEDED attempt**; output = 7 status-code
lines (200→3,100,521 …). This is the reference output everything else must match.

## 3. Exp 2a — Malformed record kills the job (~2 min)

```bash
bash scripts/run_experiment.sh 02a /results/exp02a_malformed_fail \
    MALFORMED_RECORD_EXCEPTION /data/malformed/nasa_access_malformed.log \
    -Dmapreduce.map.maxattempts=2
```
**Show in console:** `attempt_…_m_000000_0 Status : FAILED` with
`java.io.IOException: CE06 simulated failure … record=CE06_BAD_RECORD …`,
then `_1` FAILED, then `Job failed as tasks failed`.
Also `evidence/logs/application_…_0002_failure_stacktraces.log` (16 occurrences).
**Answer:** job **fails** — the poison record is in the input, so retrying runs
into it again; after `maxattempts=2` the task is permanently failed and YARN
fails the whole application (`Task failed task_…_m_000000` in RM diagnostics).

## 4. Exp 2b — Transient failure, retry recovers (~8 min)

```bash
bash scripts/run_experiment.sh 02b /results/exp02b_fail_once \
    FAIL_ONCE /data/malformed/nasa_access_malformed.log
```
**Show:** JobHistory for `job_…_0003`: 11 tasks with `0=FAILED 1=SUCCEEDED`;
counters `MALFORMED_MARKER_ENCOUNTERED=12`,
`BAD_LINE_SKIPPED_ON_RETRY=12`; output **identical** to baseline.
**Answer:** same input, but failure happens only on attempt `_0` → retry
succeeds → job completes. Distinguishes *transient* from *permanent* failure.

## 5. Exp 3a/3b — `maxattempts` knob (~3 min)

```bash
bash scripts/run_experiment.sh 03a /results/exp03a_attempts2 ALWAYS_FAIL \
    /data/clean/nasa_access_clean.log -Dmapreduce.map.maxattempts=2
bash scripts/run_experiment.sh 03b /results/exp03b_attempts4 ALWAYS_FAIL \
    /data/clean/nasa_access_clean.log -Dmapreduce.map.maxattempts=4
```
**Show side-by-side in JobHistory:**
- `job_…_0004` → `m_000000: 0=FAILED 1=FAILED` (2 attempts, job ~76 s)
- `job_…_0005` → `m_000000: 0=FAILED 1=FAILED 2=FAILED 3=FAILED` (4 attempts)
**Answer:** more attempts = more chances for a transient fault to heal, but for
a deterministic failure it only delays the inevitable job failure — and both
jobs FAIL; note more other tasks finished in 3b before the job died.

## 6. Exp 4 — Straggler with speculation OFF (~3 min)

```bash
bash scripts/run_experiment.sh 04v2 /results/exp04_spec_off SLOW_TASK \
    /data/clean/nasa_access_clean.log -Dce06.sleep.ms=400 \
    -Dmapreduce.map.speculative=false -Dmapreduce.reduce.speculative=false
```
**Show:** `job_…_0010` → `m_000000: 0=SUCCEEDED(136802ms)` — a single attempt;
job waits for it (~2.5–3 min). Log line:
`CE06-EFFECTIVE-CONF … mapreduce.map.speculative=false`.

## 7. Exp 5 — Same straggler, speculation ON (~1.5 min)

```bash
bash scripts/run_experiment.sh 05v2 /results/exp05_spec_on SLOW_TASK \
    /data/clean/nasa_access_clean.log -Dce06.sleep.ms=400 \
    -Dmapreduce.map.speculative=true \
    -Dmapreduce.job.speculative.speculativeCap=1.0 \
    -Dmapreduce.job.speculative.speculative-cap-total-tasks=1.0 \
    -Dmapreduce.job.speculative.speculative-cap-running-tasks=1.0
```
**Show (the money shot):** `job_…_0011` → `m_000000: 0=KILLED(50829ms)
1=SUCCEEDED(3327ms)`; Job Counters `Launched map tasks=13` (12+1 speculative),
`Killed map tasks=1`; job finished in **69 s vs 137 s straggler**.
**Answer:** once other maps finished and average time was known, the AM judged
attempt `_0` slow (>1.0 × average), requested a duplicate container, `_1`
completed first, RM killed `_0`. Identical output → speculation is
output-safe (only one committer wins).
**Gotcha to mention:** the default `speculative-cap-total-tasks=0.01` allows
≈0 speculative attempts for a 13-task job — an initial run with sleep=150 ms
(and slots always busy) produced no duplicate at all; caps + a longer straggler
window were required. (Evidence: `job_…_0007` vs `job_…_0011`.)

## 8. Exp 6 — `kill -9` a running attempt (~1.5 min)

```bash
bash scripts/run_kill_attempt.sh
```
**Show live:** PID discovery (`pgrep -f …attempt_…_m_000000_0`), `kill -9`,
then in JobHistory `job_…_0008`: `m_000000: 0=FAILED 1=SUCCEEDED`, and yarn
logs diagnostics:
`Container killed on request. Exit code is 137 / Killed by external signal`.
**Answer:** external process-level failure; NM reports non-zero exit → AM marks
attempt FAILED → automatic retry succeeded → job SUCCEEDED.

## 9. Exp 7 — NodeManager failure mid-job (~4 min)

```bash
bash scripts/run_nodemanager_failure.sh
```
**Narrative to show as it happens:**
1. Job RUNNING (~28 %) → `yarn --daemon stop nodemanager` → node SHUTDOWN,
   containers (incl. **the ApplicationMaster**) die; app flips to ACCEPTED with
   `cluster resource is empty` (no node to run the AM on).
2. After restart (`setsid --fork yarn --daemon start nodemanager`) a node
   registers under a **new ephemeral port**; RM allocates an AM container →
   **new MRAppMaster recovers the job from HDFS** and re-runs unfinished tasks.
3. `job_…_0009`: 7 tasks show `0=KILLED … 1000=SUCCEEDED` (post-recovery
   attempts are numbered `_1000`), job **SUCCEEDED**, output identical.
**Talking points:** AM relaunch (`yarn.resourcemanager.am.max-attempts=3`),
why attempts were KILLED (graceful stop deregisters node → RM kills its
containers), single-node limits (see report §Limitations).

## 10. Evidence wrap-up (30 s)

```bash
bash scripts/collect_evidence.sh
```
Open `evidence/REPORT_EVIDENCE_SUMMARY.txt` — one table for all 11 jobs
(state, attempts, succeeded/failed/killed) + per-task detail, RM diagnostics,
and `IDENTICAL` output verifications. Point examiner at:
- `evidence/job_history/JOB_*_task_attempts.json` — attempt-level truth
- `evidence/logs/*_run.log` — every console line incl. stack traces
- `evidence/ui/` — saved JobHistory UI pages
- `config/` — exactly the XMLs the cluster ran with
