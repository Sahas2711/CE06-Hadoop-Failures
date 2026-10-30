# CE-06 — Source Code Documentation: `FailureSimulation.java`

Single mapper/reducer job counting HTTP status codes from NASA combined-format
log lines (`host ident authuser [date] "request" status bytes`), with five
controlled failure modes. Runs under `ToolRunner`, so every Hadoop `-D` property
can be passed on the command line.

## Status extraction

```
"… 200 6245"   regex: "\"\s+(\d{3})\s+(\d+|-)\s*$"
```
Lines that don't match are counted as `UNPARSEABLE_SKIPPED` and skipped — never
silently. Injected bad lines carry the marker `CE06_BAD_RECORD`.

## Modes (`ce06.simulation.mode`, positional arg 1)

### 1. `NORMAL`
Process everything; marker/unparseable lines skipped + counted.
Baseline for Exp 1.

### 2. `MALFORMED_RECORD_EXCEPTION`  (Exp 2a)
```java
throw new IOException("CE06 simulated failure: mode=MALFORMED_RECORD_EXCEPTION …");
```
On **every** marker line, in **every** attempt → each attempt touching a marker
fails → Hadoop retries the whole task → with `mapreduce.map.maxattempts=2` the
task fails twice and the **job FAILS**. (A failure that retrying cannot fix.)

### 3. `FAIL_ONCE`  (Exp 2b)
Throw only when `attemptId.endsWith("_0")`; the retry (`_1`) sees the marker,
counts `BAD_LINE_SKIPPED_ON_RETRY`, skips it → **job SUCCEEDS** after exactly
one failed attempt per affected task. (A transient failure retrying fixes.)

### 4. `ALWAYS_FAIL`  (Exp 3a/3b)
In `setup()`, for partitions in `ce06.failing.taskpartitions` (default `0`):
```java
throw new IOException("CE06 simulated failure: mode=ALWAYS_FAIL …");
```
Fails fast, independent of input data → clean comparison of
`mapreduce.map.maxattempts=2` vs `4`: history shows exactly 2 vs 4 attempts on
`task_…_m_000000` before the job is declared FAILED.

### 5. `SLOW_TASK`  (Exps 4/5/6/7)
In `map()`, for partitions in `ce06.slow.taskpartitions` (default `0`), **only in
attempt `_0`** (retries/speculative duplicates run at full speed):
```java
if ((linesRead % ce06.sleep.every) == 0) Thread.sleep(ce06.sleep.ms);
```
- Exp 4 (spec off): attempt `_0` runs to completion → 136.8 s straggler.
- Exp 5 (spec on): scheduler launches a duplicate `_1`; `_1` finishes first,
  RM kills `_0` → `KILLED @50.8 s` + `SUCCEEDED @3.3 s`.
- Exp 6: the long `_0` window gives time to `kill -9` its JVM (exit 137).
- Exp 7: the long `_0` window gives time to stop the NodeManager mid-job.

## Counters (`CE06 Failure Simulation` group)

`LINES_READ`, `MALFORMED_MARKER_ENCOUNTERED`, `BAD_LINE_SKIPPED`,
`BAD_LINE_SKIPPED_ON_RETRY`, `UNPARSEABLE_SKIPPED`, `ALWAYS_FAIL_ATTEMPT_THROWN`.

Note: counters are only committed by **successful** attempts — after a FAILED
attempt the job-level print shows `LINES_READ=0` (failed-attempt work is
discarded; this itself is a MapReduce semantics lesson).

## Effective-config proof

`run()` prints after every job:
```
CE06-EFFECTIVE-CONF mapreduce.map.maxattempts=… mapreduce.map.speculative=… …
```
so the log itself proves which knobs the AM actually used.

## Build

```bash
bash scripts/_wsl_build.sh
# javac -cp $HADOOP_CONF_DIR:… FailureSimulation.java && jar cf build/FailureSimulation.jar *.class
```
