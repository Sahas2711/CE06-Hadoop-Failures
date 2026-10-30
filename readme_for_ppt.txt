================================================================================
README_FOR_PPT.txt  --  PRESENTATION KIT FOR CE-06
PROJECT : Failures and Speculative Execution (Hadoop MapReduce)
STUDENT : NAGAR SAHAS PRAKASH   ROLL : 202301040048
SUBJECT : CE-06 -- Big Data Analytics (Practical)
CLUSTER : Apache Hadoop 3.4.2 (HDFS + YARN + MapReduce), single node on WSL2,
          Java 17, 11 job runs covering all 7 required experiments
================================================================================
HOW TO USE: Each "SLIDE n" block = one slide. Points are paste-ready bullets.
            Numbers in [BRACKETS] are exact measured values -- quote them.
            Sections marked >>> SPEAKER NOTE <<< are for you, not the slide.


================================================================================
SLIDE 1 -- TITLE
================================================================================
- Title        : Failures and Speculative Execution
- Subtitle     : Hadoop MapReduce on a real cluster -- 7 experiments, live evidence
- Name / Roll  : NAGAR SAHAS PRAKASH / 202301040048
- Subject      : CE-06 Big Data Analytics
- One-liner    : "We broke a real Hadoop cluster on purpose -- 11 times -- and
                  proved, attempt by attempt, how YARN reacts."


================================================================================
SLIDE 2 -- AIM / PROBLEM STATEMENT
================================================================================
- Build a working Hadoop MapReduce cluster and drive one real workload through
  7 failure & speculative-execution scenarios
- Prove each behaviour with attempt-level evidence (JobHistory, YARN logs,
  ResourceManager diagnostics) -- not just screenshots of a green job
- Observe: automatic task retries -> mapreduce.map.maxattempts -> speculative
  duplicate attempts -> process kill -> NodeManager (infrastructure) failure
- Deliver reproducible scripts + evidence pack + report

>>> SPEAKER NOTE: "Every number I show comes from JobHistory REST JSON saved
    under evidence/ -- I can re-derive any claim live." <<<


================================================================================
SLIDE 3 -- CORE CONCEPTS (30-second refresher)
================================================================================
- TASK vs ATTEMPT : a task = work for one input split; an attempt = one physical
  execution. Many attempts may exist; only ONE commits output
- FAILURE TYPES   : in-band (exception in code) / out-of-band (process killed) /
  infrastructure (daemon or node gone)
- SPECULATIVE EXECUTION : if an attempt runs much slower than its peers
  (straggler), YARN launches a DUPLICATE attempt elsewhere; first finisher wins,
  the loser is KILLED; output unchanged because commit is electively single
- RETRY vs SPECULATION : retry replaces a FAILED attempt;
  speculation duplicates a HEALTHY but SLOW attempt (FAILED vs KILLED!)


================================================================================
SLIDE 4 -- SYSTEM ARCHITECTURE
================================================================================
- Windows 11 laptop (i5-12450H, 8 GB) -> WSL2 Ubuntu 26.04 -> Hadoop 3.4.2
- Daemons: NameNode :9870 | DataNode | ResourceManager :8088 |
  NodeManager :8042 | JobHistoryServer :19888  (all reachable from browser)
- Single DataNode, dfs.replication=1, 1 NodeManager: 2048 MB / 4 vCores
- 512 MB containers -> 3 concurrent map slots + 1 AM slot
- Data flow: input HDFS -> 12 map tasks -> shuffle -> 1 reduce -> HDFS output

>>> SPEAKER NOTE: If asked "why WSL, why not cloud/cluster?" -- no access to a
    multi-node lab; WSL2 gives a REAL full Hadoop (not pseudo-mode shortcuts)
    with real YARN scheduling, and everything is scripted to be re-runnable. <<<


================================================================================
SLIDE 5 -- DATASET
================================================================================
- NASA Kennedy Space Center HTTP access logs (Jul + Aug 1995)
  Source: https://ita.ee.lbl.gov/traces/
- Clean dataset : 355.8 MB, 3,461,565 records (+48 naturally malformed skipped)
- Fault-injected dataset : + 20 garbage records carrying marker CE06_BAD_RECORD
  (seed=42, deterministic, ~every 173k lines)
- Status distribution (job output, identical across all good runs):
  200 -> 3,100,521 | 302 -> 73,070 | 304 -> 266,773 | 403 -> 224 |
  404 -> 20,871 | 500 -> 65 | 501 -> 41
- KEY DESIGN: injected lines are unparseable garbage, so any run that SKIPS
  them must produce BYTE-IDENTICAL output to the clean baseline
  -> used as the correctness proof for retries & speculation


================================================================================
SLIDE 6 -- APPLICATION DESIGN (FailureSimulation.java)
================================================================================
- One MapReduce program: mapper extracts HTTP status, reducer counts
- FIVE controlled simulation modes (-Dce06.simulation.mode):
  1 NORMAL                     : baseline, bad lines skipped + counted
  2 MALFORMED_RECORD_EXCEPTION : throw IOException on every marker line
  3 FAIL_ONCE                  : throw only on first attempt, retry skips
  4 ALWAYS_FAIL                : throw in setup(), input-independent fast fail
  5 SLOW_TASK                  : sleep every 1000 lines, attempt _0 ONLY
    (retries & speculative duplicates run at full speed)
- 6 custom counters in group "CE06 Failure Simulation" (LINES_READ,
  MALFORMED_MARKER_ENCOUNTERED, BAD_LINE_SKIPPED_ON_RETRY, ...)
- Every client log prints CE06-EFFECTIVE-CONF -> proves which knobs were used


================================================================================
SLIDE 7 -- KEY CONFIGURATION (8 GB laptop tuning)
================================================================================
- dfs.replication = 1  (single DataNode)
- All daemon heaps 256 MB; NM = 2048 MB / 4 vCores; map/reduce/AM = 512 MB
- [IMPORTANT] maximum-am-resource-percent 0.1 -> 0.5
    default: 0.1 x 2048 = 204 MB  <  512 MB AM  =>  jobs STUCK in ACCEPTED
    fixed  : 0.5 x 2048 = 1024 MB >= 512 MB      =>  jobs run
- split.maxsize 32 MB -> 12 map tasks over the 356 MB file
- NM liveness expiry 30 s (fast failure demos); daemons via setsid --fork
    (survive shell exit -- otherwise NM died with its session: we hit this,
     documented, and fixed it)

>>> SPEAKER NOTE: The AM-cap bug is a great "we actually operated this cluster"
    story: symptoms -> RM diagnostic quoted -> one-line root cause -> fix. <<<


================================================================================
SLIDE 8 -- THE 7 EXPERIMENTS (map)
================================================================================
 # | Scenario                      | Job outcome we PROVED
---|-------------------------------|--------------------------------------------
 1 | Normal baseline               | SUCCEEDED, 13/13 single attempts
 2 | Malformed-record exception    | 2a FAILED (permanent) / 2b SUCCEEDED
 3 | maxattempts 2 vs 4            | exactly 2 vs exactly 4 attempts, both FAILED
 4 | Slow task, speculation OFF    | straggler waited out: 136.8 s, 1 attempt
 5 | Slow task, speculation ON     | duplicate launched; loser KILLED @50.8 s
 6 | kill -9 of running attempt    | exit code 137 -> retry -> SUCCEEDED
 7 | Stop NodeManager mid-job      | AM relaunch + recovery -> SUCCEEDED


================================================================================
SLIDE 9 -- EXPERIMENT 1: BASELINE
================================================================================
- Job job_..._0001 -- 12 maps + 1 reduce, runtime [147 s]
- JobHistory: every task has exactly 1 SUCCEEDED attempt
- Output: the 7 status-code lines (see SLIDE 5 numbers)
- Role of this run: the REFERENCE -- every other successful run must match it
  byte-for-byte (evidence/output_verification.txt: all "IDENTICAL")


================================================================================
SLIDE 10 -- EXPERIMENT 2: FAILURE + RETRIES (the core question)
================================================================================
- 2a PERMANENT failure (poison record, maxattempts=2) -> JOB FAILED
    attempt_..._m_000000_0 FAILED: java.io.IOException: CE06 simulated failure:
      ... record=CE06_BAD_RECORD ...
    attempt_..._m_000000_1 FAILED  ->  job FAILED
    RM diagnostic: "Task failed task_..._0002_m_000000;
                    Job failed as tasks failed"
    16 CE06 exceptions found in aggregated YARN logs
    WHY: the bad record is IN THE INPUT -- every retry hits it again.
         Retrying cannot fix a permanent fault => job fails. [ANSWER TO Q]
- 2b TRANSIENT failure (fail-once, same input!) -> JOB SUCCEEDED
    11 tasks show: 0=FAILED  1=SUCCEEDED
    counter BAD_LINE_SKIPPED_ON_RETRY=12
    output identical to baseline  => faulty lines excluded, not corrupted
- Take-away: SAME data, opposite outcomes -- the difference is whether the
  fault is permanent or transient. That is what retries are for.


================================================================================
SLIDE 11 -- EXPERIMENT 3: mapreduce.map.maxattempts
================================================================================
- Same deterministic failure (ALWAYS_FAIL in setup), only the knob differs:

  Job 0004, maxattempts=2 : m_000000 -> 0=FAILED 1=FAILED        (exactly 2)
  Job 0005, maxattempts=4 : m_000000 -> 0..1..2..3=FAILED        (exactly 4)

- Both jobs FAILED (deterministic fault: more attempts only DELAY failure)
- Side observation: 7 sibling maps finished in the 4-attempt run vs 5 in the
  2-attempt run before the job was killed
- Rule: extra attempts save you from RANDOM faults (disk, network, OOM),
  not from faults that happen 100% of the time


================================================================================
SLIDE 12 -- EXPERIMENTS 4 vs 5: SPECULATIVE EXECUTION  [KEY SLIDE]
================================================================================
- Controlled pair: identical input, identical straggler (partition 0 sleeps
  400 ms/1000 lines in attempt _0 only), ONLY speculation toggled

                       Exp 4 (OFF)              Exp 5 (ON)
  m_000000 attempts     0=SUCCEEDED(136.8 s)     0=KILLED(50.8 s)
                                               + 1=SUCCEEDED(3.3 s)
  Launched/Killed maps  12 / 0                   13 / 1
  Job behaviour         waited full 137 s        done in [69 s]
  Output                identical                identical

- How it happened: siblings finished -> AM saw attempt running > 1.0x average
  time -> requested duplicate container -> duplicate finished first ->
  ResourceManager KILLED the original straggler
- WHY OUTPUT IS SAFE: only the winning attempt commits; the reducer even logs
  "Ignoring obsolete output of FAILED map-task: attempt_..._m_000000_0"
- Bonus discovery (gotcha): first attempt produced NO duplicate --
  (a) all 3 map slots were busy (speculation needs a FREE container)
  (b) default speculative-cap-total-tasks=0.01 ~ 0 allowed dups on 13-task job
  Fix: caps=1.0 + longer straggler window -> duplicate at t~47 s
  Both runs kept as evidence (0007 vs 0011)


================================================================================
SLIDE 13 -- EXPERIMENT 6: PROCESS-LEVEL FAILURE (kill -9)
================================================================================
- Found the live attempt JVM: pgrep -f "...attempt_..._m_000000_0"
- kill -9 while it was sleeping
- NodeManager reported:
    "Container killed on request. Exit code is 137"
    "Killed by external signal"        (137 = 128 + SIGKILL)
- AM state machine: RUNNING -> FAILED -> launched attempt _1 -> SUCCEEDED
- Result: m_000000: 0=FAILED 1=SUCCEEDED, job SUCCEEDED, output identical
- Exam distinction: FAILED here (external kill) vs KILLED in Exp 5
  (speculative loser) -- different states, different causes


================================================================================
SLIDE 14 -- EXPERIMENT 7: NODEMANAGER FAILURE  [good war story]
================================================================================
Timeline (188 s end-to-end):
  t+0     job RUNNING (~28%), node :38791 RUNNING
  t+40 s  yarn --daemon stop nodemanager
          -> node SHUTDOWN immediately (graceful deregistration)
          -> its containers DIE, INCLUDING THE APPLICATIONMASTER
  t+40..110 s  app sits ACCEPTED; RM says:
          "Skipping AM assignment as cluster resource is empty"
  t+110 s NodeManager restarted -> registers as NEW node (fresh port)
          -> RM allocates a new AM container
          -> NEW MRAppMaster recovers job state from HDFS, re-runs tasks
  t+188 s JOB SUCCEEDED, output identical
- JobHistory signature: 7 tasks "0=KILLED ... 1000=SUCCEEDED";
  post-recovery attempts are numbered _1000
- Made possible by: yarn.resourcemanager.am.max-attempts=3 + MR AM recovery
- Honest note: on ONE node the AM dies with the NM -- so the demo shows
  AM relaunch + recovery, not retry-on-a-surviving-node (see limitations)


================================================================================
SLIDE 15 -- RESULTS OVERVIEW (all 11 jobs)
================================================================================
  Job   Experiment                        State       Attempts (succ/fail/kill)
  0001  1  Baseline                        SUCCEEDED   13/13   (13/0/0)
  0002  2a poison max=2                    FAILED      6       (0/4/2)
  0003  2b fail-once                       SUCCEEDED   25      (13/11/1)
  0004  3a ALWAYS_FAIL max=2               FAILED      6       (2/2/2)
  0005  3b ALWAYS_FAIL max=4               FAILED      13      (7/4/2)
  0010  4  spec OFF (sleep=400)            SUCCEEDED   13      (13/0/0)
  0011  5  spec ON  (sleep=400)            SUCCEEDED   14      (13/0/1)
  0008  6  kill -9                         SUCCEEDED   14      (13/1/0)
  0009  7  NodeManager failure             SUCCEEDED   20      (13/0/7)
  (0006/0007 = preliminary spec runs, kept as evidence of the gotcha)
- Output verification: ALL 5 successful experiment outputs == baseline
  (byte-identical, evidence/output_verification.txt)


================================================================================
SLIDE 16 -- EVIDENCE & VERIFICATION
================================================================================
- 106 evidence files, regenerated by: bash scripts/collect_evidence.sh
- evidence/REPORT_EVIDENCE_SUMMARY.txt  -- one-page table: every job, every
  task, every attempt (state + duration + diagnostics)
- evidence/job_history/JOB_*_task_attempts.json -- attempt-level source of truth
- evidence/job_history/JOB_*_counters.json -- MR counters (Launched=13,
  Killed=1 proves speculation)
- evidence/cluster/RM_*.json -- ResourceManager record + failure diagnostics
- evidence/logs/*_run.log -- full console output incl. stack traces
- evidence/ui/ -- saved JobHistory UI pages (screenshot substitutes)
- config/ -- exact XMLs the cluster ran with
- Live REST used by the notebook:
  localhost:19888/ws/v1/history/mapreduce/jobs/{id}/tasks/{task}/attempts


================================================================================
SLIDE 17 -- LIMITATIONS (be honest BEFORE they ask)
================================================================================
- Single node + replication=1 => cannot show: HDFS re-replication after
  DataNode loss (no second copy exists -- documented, not faked)
- Graceful NM stop deregisters instantly (SHUTDOWN); a hard crash of the ONLY
  NM also kills the ONLY AM executor => demo degenerates into AM relaunch,
  not retry-on-surviving-node. (Node LOST-via-heartbeat needs 2+ nodes.)
- No racks => no rack-aware placement / network partition demos
- Speculation needed an engineered free slot (3 map slots on one NM);
  documented the failed first attempt (job 0007) instead of hiding it
- Timings = one shared laptop kernel, not datacenter tail latency


================================================================================
SLIDE 18 -- CONCLUSION
================================================================================
- 7/7 required experiments executed on a REAL cluster with attempt-level proof
- Retries work for transient faults; permanent faults correctly fail the job;
  maxattempts governs the attempt budget (proved 2 vs 4)
- Speculation rescued a straggler: 137 s -> 69 s, loser KILLED, output UNCHANGED
- Out-of-band kill (exit 137) and infrastructure loss (whole NodeManager)
  both recovered: automatic retry and AM relaunch + HDFS job recovery
- Everything reproducible: one-command scripts + evidence pack + this notebook
- Top learning: reading JobHistory/RM diagnostics turns "it failed" into
  "it failed BECAUSE attempt _1 of task m_000000 exhausted 2 attempts at ..."


================================================================================
SLIDE 19 -- Q&A PREP (most likely questions, one-line answers)
================================================================================
 Q Task vs attempt?        A Task = split work; attempt = one execution; many
                            attempts may exist, ONE commits.
 Q Why did Exp2a fail?     A Permanent input fault: every retry re-hits the
                            poison record; after maxattempts=2 the task is dead
                            so the job dies.
 Q Why did Exp2b succeed?  A Fault only on attempt _0; retry skipped it; output
                            identical to baseline.
 Q Speculation changes results?  A No: only the winner's output commits;
                            loser is KILLED (reducer ignores its output).
 Q FAILED vs KILLED?       A FAILED = attempt errored/killed externally (137);
                            KILLED = YARN removed it (speculative loser,
                            node lost, job aborted).
 Q What is a straggler?    A Task >> slower than peers; hidden by a duplicate.
 Q Why caps=1.0?           A Default cap 0.01 rounds to ~0 speculative
                            attempts on a 13-task job.
 Q One-node limits?        A (see SLIDE 17) -- honest list.
 Q Where is the evidence?  A evidence/REPORT_EVIDENCE_SUMMARY.txt + REST.


================================================================================
APPENDIX A -- NUMBERS CHEAT SHEET (memorize these)
================================================================================
 Dataset:    3,461,565 clean | +20 injected faults | 355.8 MB | 12 splits
 Statuses:   200=3,100,521  302=73,070  304=266,773  404=20,871
 Jobs:       11 total  |  6 SUCCEEDED-of-experiment  |  3 FAILED-by-design
 Spec:       straggler 136.8 s (OFF)  vs  KILLED@50.8 s + dup 3.3 s (ON)
             job 69 s vs 137 s | Launched map tasks 13 vs 12 | Killed 1 vs 0
 Retries:    maxattempts 2 vs 4 -> exactly 2 vs exactly 4 attempts
 Kill:       exit code 137 = SIGKILL
 NM fail:    188 s total | 7 tasks re-run as attempt _1000 | AM max-attempts 3
 Cluster:    Hadoop 3.4.2 | Java 17 | 2048 MB / 4 vCores NM | 512 MB containers
             3 map slots | AM cap 0.5 (was 0.1 = stuck jobs)
 Evidence:   106 files | all outputs identical to baseline


================================================================================
APPENDIX B -- LIVE DEMO PLAN
================================================================================
2-MINUTE version (if time is short):
  1. Show evidence/REPORT_EVIDENCE_SUMMARY.txt (the table on SLIDE 15)
  2. Open JobHistory http://localhost:19888 -> job 0011 -> Tasks tab
     -> point at m_000000 having TWO attempts (KILLED + SUCCEEDED)
  3. Show the stack trace in evidence/logs/exp02a log

10-MINUTE version (run live, cluster already up via scripts/start_services.sh):
  1. Exp 5 re-run (fast, ~1.5 min) -- flip RUN=True in single.ipynb or:
       bash scripts/run_experiment.sh 05v2 /results/exp05_spec_on SLOW_TASK \
         /data/clean/nasa_access_clean.log -Dce06.sleep.ms=400 \
         -Dmapreduce.map.speculative=true -Dmapreduce.job.speculative.speculativeCap=1.0 \
         -Dmapreduce.job.speculative.speculative-cap-total-tasks=1.0 \
         -Dmapreduce.job.speculative.speculative-cap-running-tasks=1.0
     -> narrate the RM UI while it runs, then show the two attempts
  2. Exp 6 re-run (~1.5 min) -- watch it kill the JVM and recover
  3. Point to exp07 log for the NodeManager war story (too long to run live
     unless you have 4 minutes: scripts/run_nodemanager_failure.sh)

Browser bookmarks to open beforehand:
  http://localhost:8088        (ResourceManager - apps + nodes)
  http://localhost:19888       (JobHistory - job/task attempts)
  http://localhost:9870        (NameNode - blocks in /data)
  notebook: single.ipynb       (Run All works on the existing evidence)


================================================================================
APPENDIX C -- WHERE EVERYTHING LIVES
================================================================================
 Project root : D:\sem7\BDA-assignment\CE06_Hadoop_Practical\
   single.ipynb            -- this project as ONE runnable notebook
   FINAL_REPORT.md         -- full report (design, results, analysis)
   DEMONSTRATION_GUIDE.md  -- step-by-step live demo script
   VIVA_QA.md              -- 20 viva Q&A
   README.md               -- structure + quick start
   readme_for_ppt.txt      -- this presentation kit
   src\FailureSimulation.java + src\build\FailureSimulation.jar
   scripts\                -- start/stop, run_experiment, run_kill_attempt,
                              run_nodemanager_failure, collect_evidence
   config\                 -- the 4 site XMLs + capacity-scheduler used
   dataset\                -- original/clean/malformed + stats.json
   evidence\               -- 106 files (summary, JSONs, logs, UI, diffs)

 Inside WSL: /mnt/d/sem7/BDA-assignment/CE06_Hadoop_Practical


================================================================================
APPENDIX D -- FINAL-WORDS SPEAKER NOTES
================================================================================
 Opening line : "To understand Hadoop's failure handling, we didn't read about
                 it -- we broke a real cluster 11 times and let JobHistory
                 tell us what happened."
 On retries    : "Retries are cheap insurance against RANDOM faults. Against
                 DETERMINISTIC faults they're just a countdown to failure --
                 that's exactly what Exp 2a vs 2b and Exp 3 show."
 On speculation: "Speculation is a latency optimization, not a correctness
                 feature: it burns a duplicate container to buy back the tail,
                 and correctness is preserved because only one attempt commits."
 On limits     : "A single node can't fake a rack or a second copy of a block.
                 We list what we could NOT demonstrate instead of pretending."
 Closing line  : "Every slide maps to a file under evidence/ -- the report,
                 the notebook and the cluster all agree."
================================================================================
