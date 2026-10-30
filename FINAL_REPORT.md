# CE-06 — Failures and Speculative Execution
## Final Practical Report

| | |
|---|---|
| **Student** | NAGAR SAHAS PRAKASH |
| **Roll No.** | 202301040048 |
| **Course** | CE-06 — Big Data Analytics (Practical) |
| **Cluster** | Apache Hadoop **3.4.2** (HDFS + YARN + MapReduce), single node on **WSL2 Ubuntu 26.04**, Java 17 (OpenJDK 17.0.20.1) |
| **Dataset** | NASA Kennedy Space Center HTTP logs (Jul + Aug 1995), 3,461,613 records — https://ita.ee.lbl.gov/traces/ |
| **Jobs run** | 11 (all 7 required experiments; Exps 2–3 split into a/b cases; Exps 4–5 re-run as a controlled pair) |

---

## 1. Aim

Build a working Hadoop MapReduce cluster and drive one real workload through
**seven failure and speculative-execution scenarios**, proving each behaviour
with attempt-level evidence (JobHistory, YARN logs, RM diagnostics), and
document exactly what a single-node laptop setup can and cannot demonstrate.

**Learning outcomes addressed**
1. Simulate task failures → observe YARN's automatic task retries.
2. Observe `mapreduce.map.maxattempts` controlling retry behaviour and the
   success/failure decision of the job.
3. Demonstrate speculative execution off vs on (duplicate attempts, straggler
   kill) and explain why results are unaffected.
4. Handle out-of-band (process kill) and infrastructure (NodeManager) failures.
5. Collect reproducible evidence and reason honestly about local limits.

---

## 2. Environment & configuration

Audited first (`scripts/environment_audit.ps1` → `evidence/reports/environment_audit_report.txt`):
Windows 11, i5-12450H (8C/12T), 7.65 GB RAM, C: 17 GB / D: 47 GB free; WSL2
Ubuntu 26.04; Hadoop tarball copied to `/opt/hadoop-3.4.2`; namenode formatted
empty at `/opt/hadoop-data/namenode`.

**Tuning for an 8 GB laptop** (all in `config/`):

| Setting | Value | Why |
|---|---|---|
| `dfs.replication` | 1 | single DataNode |
| NN/DN/RM/NM/JHS heaps | 256 m | fit in WSL's ~3.6 GB |
| `yarn.nodemanager.resource.memory-mb` | 2048 (4 vCores) | leave room for daemons |
| map/reduce container MB | 512 (Xmx400m) | 3 concurrent map slots + AM |
| AM container MB | 512 | fits scheduler limits below |
| `maximum-am-resource-percent` | **0.5** (was 0.1) | 0.1×2048=204 MB < 512 MB AM → jobs stuck ACCEPTED forever |
| `mapreduce.input.fileinputformat.split.maxsize` | 32 MB | 12 splits of the 356 MB file → 12 map tasks |
| `yarn.nodemanager.liveness-monitor.expiry-interval-secs` | 30 | fast node-failure demos |
| daemons started via | `setsid --fork` | survive shell/session exit |

All five daemons verified from Windows browser (HTTP 200): NN 9870, RM 8088,
NM 8042, JHS 19888.

---

## 3. Dataset preparation

`scripts/prepare_dataset.py` (Python 3.14, seed=42) parses the NASA combined
format with `"$status bytes$"` tail regex and produces:

| File | Records | Notes |
|---|---|---|
| `clean/nasa_access_clean.log` (355.8 MB) | 3,461,565 clean | + 48 naturally malformed skipped & counted |
| `malformed/nasa_access_malformed.log` (359.1 MB) | above + **20 injected** | marker `CE06_BAD_RECORD`, pure garbage lines, ~every 173 k lines |

Status distribution (identical in both; the job's baseline output):
`200→3,100,521 · 302→73,070 · 304→266,773 · 403→224 · 404→20,871 · 500→65 · 501→41`

Because injected lines are unparseable garbage, a run that **skips** them
(Exp 2b) must produce output **byte-identical** to the clean baseline — used as
a correctness proof throughout (`evidence/output_verification.txt`).

---

## 4. Application

`src/FailureSimulation.java` — mapper extracts the HTTP status code, reducer
sums counts, plus five modes (full docs: `src/README.md`):

| Mode | Behaviour | Used by |
|---|---|---|
| `NORMAL` | skip+count bad lines | Exp 1 |
| `MALFORMED_RECORD_EXCEPTION` | `IOException` on **every** marker line, every attempt | Exp 2a |
| `FAIL_ONCE` | throw only in attempt `_0`, skip marker on retry | Exp 2b |
| `ALWAYS_FAIL` | throw in `setup()` on partition 0 (input-independent) | Exp 3a/3b |
| `SLOW_TASK` | `Thread.sleep` every 1000 lines on partition 0, **only attempt `_0`** | Exps 4/5/6/7 |

Counter group `CE06 Failure Simulation`: `LINES_READ`,
`MALFORMED_MARKER_ENCOUNTERED`, `BAD_LINE_SKIPPED`, `BAD_LINE_SKIPPED_ON_RETRY`,
`UNPARSEABLE_SKIPPED`, `ALWAYS_FAIL_ATTEMPT_THROWN`. The client prints
`CE06-EFFECTIVE-CONF …` so each log proves which knobs were actually used.

---

## 5. Experiments & results

Evidence for every row: `evidence/REPORT_EVIDENCE_SUMMARY.txt` (table below is
its distilled form), per-job JSON in `evidence/job_history/JOB_<id>_*.json`,
console logs in `evidence/logs/`, RM diagnostics in `evidence/cluster/`.

### Results overview — 11 jobs

| Job | Exp | Outcome | Attempts (key task) | Job counters / evidence |
|---|---|---|---|---|
| `…_0001` | 1 Baseline `NORMAL` | **SUCCEEDED** | 13 tasks × 1 SUCCEEDED | `LINES_READ=3,461,565`; 147 s |
| `…_0002` | 2a Poison record, max=2 | **FAILED** | `m_000000: 0=FAILED 1=FAILED` | RM: `Task failed task_…_0002_m_000000`; 16 CE06 stack traces in yarn logs; 105 s |
| `…_0003` | 2b Fail-once | **SUCCEEDED** | 11 tasks `0=FAILED 1=SUCCEEDED` (25 attempts) | `BAD_LINE_SKIPPED_ON_RETRY=12`; output ≡ baseline |
| `…_0004` | 3a `ALWAYS_FAIL` max=2 | **FAILED** | `m_000000: 0=FAILED 1=FAILED` | 2 attempts exactly; 76 s |
| `…_0005` | 3b `ALWAYS_FAIL` max=4 | **FAILED** | `m_000000: 0=…1=…2=…3=FAILED` | 4 attempts exactly; more other tasks finished first; 76 s |
| `…_0010` | 4 Straggler, spec **OFF** | **SUCCEEDED** | `m_000000: 0=SUCCEEDED(136,802 ms)` | single attempt; full 137 s straggler waited out |
| `…_0011` | 5 Straggler, spec **ON** | **SUCCEEDED** | `m_000000: 0=KILLED(50,829 ms) 1=SUCCEEDED(3,327 ms)` | `Launched map tasks=13`, `Killed map tasks=1`; job **69 s** |
| `…_0008` | 6 `kill -9` attempt | **SUCCEEDED** | `m_000000: 0=FAILED 1=SUCCEEDED` | `Exit code is 137 / Killed by external signal`; 72 s |
| `…_0009` | 7 NodeManager stop | **SUCCEEDED** | 7 tasks `0=KILLED 1000=SUCCEEDED`, 5 tasks `1000=SUCCEEDED` (20 attempts) | AM relaunch + recovery; 188 s end-to-end |
| `…_0006` | 4 preliminary (sleep 150) | SUCCEEDED | 1 attempt | superseded by 0010 |
| `…_0007` | 5 preliminary (sleep 150) | SUCCEEDED | **no duplicate** | documented gotcha (see §5.5) |

### 5.1 Exp 1 — Baseline
12 maps + 1 reduce, every task a single successful attempt, output = 7 status
lines. Reference for all output comparisons.

### 5.2 Exp 2 — Malformed-record failure and retries
- **2a (job 0002, maxattempts=2):** mapper throws
  `java.io.IOException: CE06 simulated failure: mode=MALFORMED_RECORD_EXCEPTION
  attempt=attempt_…_0002_m_000000_0 record=CE06_BAD_RECORD …` (client log +
  16 occurrences in aggregated yarn logs). `_0` and `_1` both FAILED →
  **job FAILED**: *"Job failed as tasks failed. failedMaps:1"*. Running
  attempts of other tasks were KILLED at that moment.
  **Explanation:** the faulty record lives in HDFS input, so every retry hits
  it again — a *permanent* failure. Retries buy nothing; the job fails.
- **2b (job 0003, FAIL_ONCE):** the same input succeeds because only the first
  attempt of each task throws; retries skip the marker. JobHistory shows
  `0=FAILED 1=SUCCEEDED` on 11 tasks (12th had no marker; `m_000010` even got a
  speculative `_2=KILLED`). **Job SUCCEEDED**, counters
  `MALFORMED_MARKER_ENCOUNTERED=12`, `BAD_LINE_SKIPPED_ON_RETRY=12`, and output
  **identical to baseline** → the faulty records were excluded, not corrupted.

### 5.3 Exp 3 — `mapreduce.map.maxattempts`
Deterministic `ALWAYS_FAIL` (throws in `setup()`, same input both runs):
max=2 → task `m_000000` recorded **exactly 2 FAILED attempts**, job FAILED;
max=4 → **exactly 4 FAILED attempts**, job FAILED. Confirms the knob governs
per-task attempt budget; job outcome for a deterministic fault is failure
either way — more attempts only delay it (and, incidentally, let 7 sibling maps
finish in 3b vs 5 in 3a).

### 5.4 Exps 4 & 5 — Speculative execution (controlled pair)
Identical workload: `SLOW_TASK`, partition 0 sleeps 400 ms per 1000 lines in
attempt `_0` only (≈296 k lines ⇒ ~118 s of artificial delay), speculation the
only variable:

| | Exp 4 — OFF (`0010`) | Exp 5 — ON (`0011`) |
|---|---|---|
| `m_000000` attempts | `0=SUCCEEDED(136,802 ms)` — alone | `0=KILLED(50,829 ms)`, `1=SUCCEEDED(3,327 ms)` |
| Launched/Killed map tasks | 12 / 0 | **13 / 1** |
| Effect | job blocked until straggler finished | duplicate finished first; RM killed the straggler |
| Output | ≡ baseline | ≡ baseline |

**Explanation:** once sibling maps completed, the AM saw the running attempt at
≫ 1.0 × average attempt time, requested a duplicate container, the duplicate
completed in 3.3 s, and YARN KILLED the original. Only one attempt commits, so
results are unchanged — speculation trades wasted compute for latency.

### 5.5 Gotcha found (and documented)
The preliminary spec-ON run (`0007`, sleep 150 ms) produced **no duplicate**.
Diagnosis: (a) the straggler finished while every map slot was still busy with
fast tasks — speculation needs a *free* container; (b) defaults
`speculative-cap-total-tasks=0.01` / `speculative-cap-running-tasks=0.1` can
round to zero allowed duplicates on a 13-task job. The re-run set all caps to
1.0 and lengthened the straggler window — duplicate appeared at t≈47 s.
Both runs are kept as evidence (`0007` vs `0011` conf in JobHistory).

### 5.6 Exp 6 — Process-level failure (`kill -9`)
While `attempt_…_0008_m_000000_0` slept, its JVM (found via
`pgrep -f …attempt_…_m_000000_0`) was killed. NM reported:
`Container killed on request. Exit code is 137` / `Killed by external signal`;
AM transitioned `RUNNING → FAILED`, launched `_1`, which succeeded. Job
**SUCCEEDED**; shuffle log shows the reducer
`Ignoring obsolete output of FAILED map-task: attempt_…_m_000000_0`.
Contrast with Exp 5: FAILED (external kill) vs KILLED (speculative loser).

### 5.7 Exp 7 — NodeManager failure mid-job
Timeline (`evidence/logs/exp07_nodemanager_failure_run.log`):

1. **t+0** job submitted; attempt `_0`s running (straggler window ~70 s); node
   `Sahas.localdomain:38791 RUNNING`.
2. **t+40 s** `yarn --daemon stop nodemanager` → node **SHUTDOWN** immediately
   (graceful deregistration); NM took its containers — **including the AM** —
   with it; jps shows no MRAppMaster.
3. **t+40…110 s** application sits **ACCEPTED**, RM diagnostic:
   *"Skipping AM assignment as cluster resource is empty … Queue Resource Limit
   for AM = \<memory:0\>"* (no node to host the AM).
4. **t+110 s** NodeManager restarted → registers as **new node
   `Sahas.localdomain:46549`** (fresh ephemeral port; old ID stays SHUTDOWN) →
   RM allocates an AM container → **new MRAppMaster recovers job state from
   HDFS** and re-runs unfinished tasks.
5. **t+188 s** job **SUCCEEDED**. JobHistory: 7 tasks
   `0=KILLED … 1000=SUCCEEDED` (KILLED = RM released them with the lost node;
   post-recovery attempts are numbered `_1000`), 5 tasks only `1000=SUCCEEDED`
   (their pre-crash attempt events died with the first AM — history is only as
   durable as the AM that writes it). Output ≡ baseline.

**Why the job succeeded:** `yarn.resourcemanager.am.max-attempts=3` permitted an
AM relaunch, MR AM recovery replayed job/attempt state from the staging dir, and
attempt retries re-processed unfinished splits. Not every outcome is success in
general — see limits.

---

## 6. Evidence inventory

| Artifact | Path |
|---|---|
| One-page results table (all jobs, per-task attempts) | `evidence/REPORT_EVIDENCE_SUMMARY.txt` |
| Attempt-level JSON (id, state, duration, diagnostics) | `evidence/job_history/JOB_<jobid>_task_attempts.json` |
| Job details + MR counters (incl. `Job Counters` group) | `evidence/job_history/JOB_<jobid>_{detail,counters}.json` |
| Console logs (progress, exceptions, effective conf, RESULT line) | `evidence/logs/*_run.log` |
| Aggregated stack traces of failed attempts | `evidence/logs/application_*_failure_stacktraces.log` |
| RM application records + diagnostics | `evidence/cluster/RM_<appid>.json` |
| Cluster/node snapshots | `evidence/cluster/cluster_snapshot.txt` |
| JobHistory UI pages (HTML snapshots) | `evidence/ui/` |
| Baseline-vs-all output diff (all `IDENTICAL`) | `evidence/output_verification.txt` |
| Configs actually used | `config/` |
| Environment audit | `evidence/reports/environment_audit_report.txt` |

Regenerate everything: `bash scripts/collect_evidence.sh`.

---

## 7. What can and cannot be demonstrated locally (honest limits)

**Demonstrated:** in-band application exceptions (Exp 2), retry budgets (Exp 3),
speculative duplicate + straggler kill (Exp 5), out-of-band SIGKILL (Exp 6),
daemon/node loss + AM relaunch + job recovery (Exp 7), counter and output
semantics, scheduler capacity interactions.

**Not honestly demonstrable on one node / replication=1:**
- **Node LOST via heartbeat expiry** — a *graceful* stop deregisters instantly
  (SHUTDOWN); a hard crash of the only NM also kills the only AM executor, so
  the failure degenerates into AM relaunch rather than task-attempt retry on
  surviving nodes.
- **Multi-node rack/network partitions, rack-aware placement** — requires ≥2 racks.
- **HDFS re-replication after DataNode loss** — with `dfs.replication=1` and one
  DN there is no second copy; block loss would occur (documented, not staged).
- **Speculative duplicates on a busy cluster** — with 3 map slots, speculation
  needs a genuinely free slot; we engineered the window rather than relying on
  load (see §5.5).
- **Multi-node parallelism / realistic tail latency** — all containers share one
  kernel; timings reflect a laptop, not a datacenter.

Workarounds used: single-node-safe scenarios, forced straggler windows,
`maximum-am-resource-percent=0.5`, and explicit config capture so every claim
is traceable.

---

## 8. Conclusion

All seven required experiments were executed on a real Hadoop 3.4.2 cluster
with attempt-level evidence: retries observed (2a/2b), `maxattempts` proven to
control attempt counts and job verdict (2 vs 4 attempts), speculation shown
switching from a 137 s straggler wait to a duplicate-attempt rescue (KILLED at
50.8 s, job 69 s) with unchanged results, a live JVM kill recovered via exit
code 137, and a full NodeManager outage survived through AM relaunch and job
recovery — with byte-identical outputs across every successful run and clear,
documented limits of the single-node setup.

---

## 9. Appendix — reproduction cheat-sheet

```bash
wsl -d Ubuntu -u root
cd /mnt/d/sem7/BDA-assignment/CE06_Hadoop_Practical
bash scripts/start_services.sh                # 5 daemons (setsid-detached)
bash scripts/run_all_experiments.sh           # Exps 1–5 (first pass)
bash scripts/run_experiment.sh 04v2 /results/exp04_spec_off SLOW_TASK \
    /data/clean/nasa_access_clean.log -Dce06.sleep.ms=400 \
    -Dmapreduce.map.speculative=false -Dmapreduce.reduce.speculative=false
bash scripts/run_experiment.sh 05v2 /results/exp05_spec_on SLOW_TASK \
    /data/clean/nasa_access_clean.log -Dce06.sleep.ms=400 \
    -Dmapreduce.map.speculative=true \
    -Dmapreduce.job.speculative.speculativeCap=1.0 \
    -Dmapreduce.job.speculative.speculative-cap-total-tasks=1.0 \
    -Dmapreduce.job.speculative.speculative-cap-running-tasks=1.0
bash scripts/run_kill_attempt.sh              # Exp 6
bash scripts/run_nodemanager_failure.sh       # Exp 7 (NM restart is setsid'd)
bash scripts/collect_evidence.sh              # rebuild evidence/summary
```

Walkthrough for the examiner: `DEMONSTRATION_GUIDE.md` ·
Questions: `VIVA_QA.md` · Code: `src/README.md`.
