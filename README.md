# CE-06 — Failures and Speculative Execution

[![Hadoop](https://img.shields.io/badge/Hadoop-3.4.2-orange)](https://hadoop.apache.org/)
[![MapReduce](https://img.shields.io/badge/MapReduce-Java-green)](#srcfailureSimulationjava)
[![Platform](https://img.shields.io/badge/Platform-WSL2%20%2F%20Ubuntu-blue)](#quick-start-cluster-already-configured)
[![License](https://img.shields.io/badge/License-Educational-lightgrey)](#license)

**Practical:** CE-06 (Big Data Analytics) — *Failures and Speculative Execution*
**Student:** NAGAR SAHAS PRAKASH · Roll No. 202301040048
**Cluster:** Hadoop 3.4.2 (HDFS + YARN + MapReduce) on WSL2/Ubuntu, single node
**Dataset:** NASA Kennedy Space Center HTTP logs (Jul+Aug 1995), 3,461,613 records

## What this project demonstrates

A single MapReduce job (HTTP status-code word count over NASA logs) driven through
**seven failure / speculative-execution scenarios**, with full evidence captured:

| Exp | Scenario | Mode / Knob | Outcome |
|-----|----------|-------------|---------|
| 1 | Normal baseline | `NORMAL` | SUCCEEDED, 13/13 attempts succeed |
| 2a | Malformed-record exception, `maxattempts=2` | `MALFORMED_RECORD_EXCEPTION` | **FAILED** — every attempt hits `CE06_BAD_RECORD`, retries exhausted |
| 2b | Fail once, retry succeeds | `FAIL_ONCE` | SUCCEEDED — attempt `_0` FAILED, `_1` SUCCEEDED; output identical to baseline |
| 3a | Always-fail, `maxattempts=2` | `ALWAYS_FAIL` | FAILED after exactly **2** attempts on `m_000000` |
| 3b | Always-fail, `maxattempts=4` | `ALWAYS_FAIL` | FAILED after exactly **4** attempts on `m_000000` |
| 4 | Slow task, speculation **OFF** | `SLOW_TASK`, `-Dmapreduce.map.speculative=false` | SUCCEEDED — straggler ran **136.8 s** alone, 1 attempt |
| 5 | Slow task, speculation **ON** | `SLOW_TASK`, spec on + caps=1.0 | SUCCEEDED — duplicate attempt launched: `_0` **KILLED @50.8 s**, `_1` SUCCEEDED @3.3 s; job **69 s** |
| 6 | `kill -9` a running attempt JVM | external process kill | SUCCEEDED — exit code **137**, `_0=FAILED`, `_1=SUCCEEDED` |
| 7 | Stop NodeManager mid-job, restart | `yarn --daemon stop/start nodemanager` | SUCCEEDED — node SHUTDOWN, AM killed, **AM relaunched + recovered**, 7 attempts KILLED then re-run as `attempt_…_1000` |

Every successful run's output is **byte-identical to the baseline** (verified by
`hdfs dfs -cat | diff`, see `evidence/output_verification.txt`).

## Repository layout

```
CE06_Hadoop_Practical/
├── README.md                     ← this file
├── single.ipynb                  ← the WHOLE project as ONE runnable notebook (REST evidence, Run All works)
├── readme_for_ppt.txt            ← presentation kit: 18-slide deck content + demo plans + cheat sheets
├── DEMONSTRATION_GUIDE.md        ← step-by-step live demo script (examiner-facing)
├── FINAL_REPORT.md               ← full report: design, results, analysis, learning outcomes
├── VIVA_QA.md                    ← viva questions + model answers
├── src/
│   ├── FailureSimulation.java    ← MapReduce app (5 simulation modes)
│   ├── build/FailureSimulation.jar
│   └── README.md                 ← code-level documentation
├── scripts/
│   ├── environment_audit.ps1     ← Phase-1 Windows/WSL audit
│   ├── setup_hadoop.sh           ← idempotent cluster install/configure/format
│   ├── start_services.sh / stop_services.sh   ← daemons via setsid (survive shell exit)
│   ├── prepare_dataset.py        ← builds clean + malformed datasets (seed=42)
│   ├── download_dataset.sh       ← fetches NASA logs from ita.ee.lbl.gov
│   ├── run_experiment.sh         ← runs ONE experiment + captures evidence
│   ├── run_all_experiments.sh    ← Exps 1–5
│   ├── run_kill_attempt.sh       ← Exp 6
│   ├── run_nodemanager_failure.sh← Exp 7
│   ├── collect_evidence.sh       ← gathers ALL evidence into evidence/
│   └── _wsl_*.sh                 ← internal helpers (build/upload/status)
├── config/                       ← the four site XMLs + capacity-scheduler + env used
├── dataset/
│   ├── original/  clean/  malformed/  stats.json  README.md
└── evidence/
    ├── REPORT_EVIDENCE_SUMMARY.txt   ← one-page results table (all jobs)
    ├── logs/                          ← per-experiment console logs + stack traces
    ├── job_history/JOB_*_*.json       ← attempt states, counters, job details (REST)
    ├── cluster/                       ← RM app records + diagnostics, node snapshots
    ├── ui/                            ← JobHistory UI HTML pages
    └── output_verification.txt        ← baseline-vs-every-run diff results
```

## Quick start (cluster already configured)

```bash
# inside WSL (Ubuntu 26.04)
sudo bash scripts/start_services.sh          # NN, DN, RM, NM, JobHistory
bash scripts/run_all_experiments.sh          # Exps 1–5
bash scripts/run_kill_attempt.sh             # Exp 6
bash scripts/run_nodemanager_failure.sh      # Exp 7
bash scripts/collect_evidence.sh             # rebuild evidence/REPORT_EVIDENCE_SUMMARY.txt
```

Key web UIs (from Windows browser): NameNode `http://localhost:9870`,
ResourceManager `http://localhost:8088`, NodeManager `http://localhost:8042`,
JobHistory `http://localhost:19888`.

## Building the MapReduce job

```bash
bash scripts/_wsl_build.sh
# equivalent to:
#   javac -cp $HADOOP_CONF_DIR:$(hadoop classpath) src/FailureSimulation.java
#   jar cf src/build/FailureSimulation.jar -C src/build/classes .
```

A prebuilt `src/build/FailureSimulation.jar` is committed for convenience.

## Regenerating the datasets

The large processed logs (`dataset/clean/`, `dataset/malformed/`, ~700 MB total)
are **not stored in this repo** (GitHub 100 MB file limit). They are fully
reproducible from the committed original archives:

```bash
# downloads already present in dataset/original/; otherwise:
#   bash scripts/download_dataset.sh
python3 scripts/prepare_dataset.py    # seed=42, deterministic output
```

## Simulation knobs (GenericOptionsParser `-D`)

| Property | Meaning |
|----------|---------|
| `ce06.simulation.mode` | `NORMAL`, `MALFORMED_RECORD_EXCEPTION`, `FAIL_ONCE`, `ALWAYS_FAIL`, `SLOW_TASK` |
| `ce06.sleep.ms` / `ce06.sleep.every` | straggler delay: sleep `ms` every N lines (attempt `_0` only) |
| `ce06.slow.taskpartitions` / `ce06.failing.taskpartitions` | which partitions are slow / always fail |
| `mapreduce.map.maxattempts` | retries per task (Exp 3) |
| `mapreduce.map.speculative` | speculation on/off (Exp 4/5) |
| `mapreduce.job.speculative.speculative-cap-total-tasks` etc. | spec capacity caps (set to 1.0 so small jobs can speculate) |

## Key results at a glance

| Question | Answer (from evidence) |
|----------|------------------------|
| Does retry fix a transient failure? | **Yes** — Exp 2b: attempt `_0` FAILED, `_1` SUCCEEDED, output identical to baseline |
| Does retry fix a permanent failure? | **No** — Exp 2a/3a: job FAILED after exactly `maxattempts` tries |
| Does `maxattempts` control retry count? | **Yes** — Exp 3a (`=2`) vs 3b (`=4`): exactly 2 vs 4 attempts on `m_000000` |
| What does speculation do for stragglers? | Exp 4 (off): **136.8 s** straggler; Exp 5 (on): duplicate launched, `_0` KILLED @50.8 s, job **69 s** |
| Can you kill an attempt and still succeed? | **Yes** — Exp 6: `kill -9` → exit 137, retry succeeded |
| Does YARN recover a lost NodeManager? | **Yes** — Exp 7: node SHUTDOWN, AM killed and **relaunched**, job recovered |

Full data: [`evidence/REPORT_EVIDENCE_SUMMARY.txt`](evidence/REPORT_EVIDENCE_SUMMARY.txt) ·
Analysis: [`FINAL_REPORT.md`](FINAL_REPORT.md) ·
Viva prep: [`VIVA_QA.md`](VIVA_QA.md) ·
Live demo: [`DEMONSTRATION_GUIDE.md`](DEMONSTRATION_GUIDE.md)

## License

Educational use — coursework submission for CE-06 (Big Data Analytics).
NASA HTTP logs are public-domain data from the
[Lawrence Berkeley Lab Internet Traffic Archive](https://ita.ee.lbl.gov/).
