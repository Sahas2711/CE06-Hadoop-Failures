# CE-06 — Viva Questions & Model Answers

*Failures and Speculative Execution — Hadoop 3.4.2 single-node cluster, NASA HTTP logs*

## A. Concepts & architecture

**Q1. What is a task attempt vs a task?**
A *task* (e.g. `task_…_m_000000`) is the unit of work for one input split; an
*attempt* (`attempt_…_m_000000_0/_1/…`) is one physical execution of it. A task
can have many attempts (retries, speculation); only one commits output.

**Q2. What is speculative execution and why does it exist?**
YARN/MR detects *stragglers* — tasks running much longer than peers (skewed
data, slow disk, hot core). To hide the tail latency the AM launches a
**duplicate attempt** of the same task on another container; whichever
attempt finishes first wins and the other is **KILLED**. Output stays correct
because committers elect one winner. Ours: `m_000000` `_0` KILLED @50.8 s,
`_1` SUCCEEDED @3.3 s (`job_…_0011`).

**Q3. Which condition triggers speculation in your job?**
Attempt runtime > `slowtaskthreshold` (1.0) × average completed attempt time for
that task type, speculation enabled for the task type, no existing duplicate,
capacity under `speculative-cap-total-tasks` / `speculative-cap-running-tasks`,
and a **free container** on the cluster. All five had to hold — our first run
(`job_…_0007`) produced no duplicate because slots were continuously occupied
and the caps defaulted to 0.01.

**Q4. When is speculation NOT useful / disabled?**
Reduce speculation is off by default here (`mapreduce.reduce.speculative=false`)
and we kept it off: shuffle-phase duplicates waste network. Also useless when
all tasks are equally slow (cluster-wide problem), when jobs are tiny/uberized,
or when tasks are non-idempotent without commit protocols.

**Q5. Retry vs speculation — same thing?**
No. A **retry** replaces a *failed* attempt (exit code ≠ 0, exception, lost
container). **Speculation** duplicates a *healthy but slow* attempt; the loser
is KILLED, not FAILED. Evidence contrast: Exp 6 `_0=FAILED` (killed JVM, exit
137) vs Exp 5 `_0=KILLED` (speculative loser).

## B. The experiments (evidence-based)

**Q6. Exp 2a: why did the job fail even though Hadoop retries tasks?**
The exception is thrown on a real input record (`CE06_BAD_RECORD`). Every
retry re-reads the same split and hits the same record → deterministic
failure. After `mapreduce.map.maxattempts=2` attempts, the task is permanently
failed → AM fails the job: RM diagnostic
`Task failed task_…_0002_m_000000; Job failed as tasks failed`.

**Q7. Exp 2b: why did the same kind of exception NOT fail the job?**
`FAIL_ONCE` throws only in attempt `_0`; the retry `_1` skips the marker
(`BAD_LINE_SKIPPED_ON_RETRY=12`) and completes. 11 tasks show
`0=FAILED 1=SUCCEEDED`; job SUCCEEDED with output identical to baseline —
the definition of a *transient* failure.

**Q8. What exactly did Exps 3a/3b compare?**
Same deterministic failure (`ALWAYS_FAIL` in `setup()` on partition 0), only
`mapreduce.map.maxattempts` differed: 2 vs 4. JobHistory shows
`m_000000: 0=FAILED 1=FAILED` (3a) vs `0=… 1=… 2=… 3=FAILED` (3b). Both jobs
FAILED — more attempts only *delay* failure for deterministic faults; they
matter for probabilistic faults. Side effect: 3b let 7 other maps finish vs
5 in 3a before the job was killed.

**Q9. Exp 4 vs 5 — what was controlled and what changed?**
Identical input, split layout, sleep workload (400 ms / 1000 lines on partition
0's first attempt), cluster — only `mapreduce.map.speculative` differed.
Result: OFF → single attempt `0=SUCCEEDED(136802ms)`; ON → duplicate launched,
`0=KILLED(50829ms) 1=SUCCEEDED(3327ms)`, `Launched map tasks=13`,
`Killed map tasks=1`, job 69 s. Output byte-identical in both.

**Q10. Exp 6: what does exit code 137 mean?**
128 + 9 = SIGKILL. We `kill -9`'d the YarnChild JVM; the NM reported
`Container killed on request. Exit code is 137 / Killed by external signal`;
AM transitioned the attempt `RUNNING → FAILED` and scheduled attempt `_1`,
which succeeded. Demonstrates **out-of-band process failure**, as opposed to
in-band application exceptions (Exp 2) or infrastructure loss (Exp 7).

**Q11. Exp 7: the ApplicationMaster itself runs on the NodeManager we stopped —
what happened?**
Graceful NM stop killed its containers including the AM. The application never
went to FAILED because `yarn.resourcemanager.am.max-attempts=3` allowed an AM
**relaunch**: while the node was down the app sat ACCEPTED
(`cluster resource is empty`). After restart, RM allocated a new AM container
on the re-registered node; the new MRAppMaster **recovered job state from HDFS**
and re-ran the 7 unfinished tasks (their `_0` attempts were KILLED when the node
left; replacements are numbered `…_1000`). Job SUCCEEDED, output identical.

**Q12. Why did some Exp 7 tasks show attempt `_1000` and some only `_1000`?**
Post-recovery attempts use attempt numbers ≥1000 to keep IDs unique across AM
generations. Tasks already finished or whose pre-crash attempt events were lost
(history is only as durable as the AM that wrote it — our first AM died
abruptly) show just the `_1000` attempt. Honest limitation, documented.

**Q13. You stopped the NodeManager "safely" — how does that differ from a crash?**
A graceful stop *deregisters* the node (state SHUTDOWN immediately, RM stops
scheduling to it); a hard crash is detected only after heartbeat expiry
(`yarn.nodemanager.liveness-monitor.expiry-interval-secs`, we set 30 s) where
the node goes LOST. Containers are killed in both cases from the job's
viewpoint; only detection latency differs. On one physical host we cannot
show node loss — the NM restart also gets a new ephemeral port, so RM treats
it as a **new node** (`:46549`/`:35503` RUNNING while old IDs stay SHUTDOWN).

## C. Configuration & internals

**Q14. Which settings did you tune for an 8 GB laptop and why?**
NN/DN/RM/NM/JobHistory heaps 256 m; NM resource 2048 MB / 4 vCores; map &
reduce containers 512 MB (400 m heap); AM 512 MB;
`maximum-am-resource-percent=0.5` (default 0.1 → 204 MB < 512 MB AM would
leave jobs stuck ACCEPTED forever); split maxsize 32 MB → 12 mappable splits;
`dfs.replication=1`; `yarn.nodemanager.liveness-monitor.expiry-interval-secs=30`
for fast failure demos.

**Q15. Why did jobs start ACCEPTED before you changed one property?**
CapacityScheduler reserves at most `maximum-am-resource-percent` of queue memory
for ApplicationMasters. 0.1 × 2048 = 204 MB, but our AM requests 512 MB → no AM
could ever start. 0.5 × 2048 = 1024 ≥ 512 ✓. (RM diagnostic quoted in report.)

**Q16. Where do you see all this evidence in the UI?**
RM `localhost:8088` (apps, diagnostics, nodes), JobHistory `localhost:19888`
→ job page → Tasks tab (attempt IDs + states + durations), per-attempt logs via
`yarn logs -applicationId …`, and REST:
`/ws/v1/history/mapreduce/jobs/<job>/tasks/<task>/attempts`.

**Q17. Why are failed attempts' counters missing (LINES_READ=0 in Exp 2a)?**
Counters are committed by an attempt's *successful* commit protocol; a failed
attempt's counters are discarded on retry — work is replayed, not accumulated.
Successful jobs show real totals (Exp 2b: `LINES_READ=6923150` because failed
`_0` attempts re-read their splits before `_1` committed).

**Q18. Map output correctness with retries/speculation — how?**
Task outputs are written to per-attempt work directories and committed
atomically at task completion (`_SUCCESS` marker + rename); losing attempts'
dirs are cleaned. The reduce side only consumes committed outputs
(log: `Ignoring obsolete output of FAILED map-task: attempt_…_m_000000_0`).
Hence all our successful runs produced byte-identical results.

**Q19. What can a single-node laptop NOT demonstrate honestly?**
Rack-aware placement / multi-rack network faults; DataNode replication repair
(below `dfs.replication` there is nothing to re-replicate from — we use
replication=1 and document it); node LOST via heartbeat expiry (graceful stop
deregisters instead); multi-node parallel speculation (only 4 containers exist
— duplicates need a genuinely free slot); HDFS safe-mode recovery with real
edits under load. All are explicitly reported as environment limits.

**Q20. If the examiner asks "so what did YOU build?"**
One MapReduce program with 5 failure/slowdown modes + counter instrumentation;
idempotent cluster installer; dataset builder with deterministic fault
injection; 11 instrumented job runs covering 7 required experiments; an
evidence pipeline (REST + logs + UI snapshots + output diffs) that proves each
learning outcome with attempt-level data — see `FINAL_REPORT.md`.
