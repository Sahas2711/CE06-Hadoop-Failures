#!/bin/bash
# =============================================================================
# CE-06: Run ONE experiment with full evidence capture.
# Usage: run_experiment.sh <exp_id> <hdfs_output> <mode> <input_hdfs> [extra_job_args...]
# Example: run_experiment.sh 01 /results/exp01_baseline NORMAL /data/clean/nasa_access_clean.log
# Evidence -> evidence/logs/expNN_*.log , evidence/job_history/
# =============================================================================
set -u
PROJ=/mnt/d/sem7/BDA-assignment/CE06_Hadoop_Practical
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_HOME=/opt/hadoop-3.4.2
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export HADOOP_YARN_HOME=$HADOOP_HOME
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

EXP=$1; OUT=$2; MODE=$3; INPUT=$4; shift 4
JAR=$PROJ/src/build/FailureSimulation.jar
LOGDIR=$PROJ/evidence/logs
JHDIR=$PROJ/evidence/job_history
mkdir -p "$LOGDIR" "$JHDIR"
LOG=$LOGDIR/exp${EXP}_$(echo "$MODE" | tr 'A-Z' 'a-z')_run.log

hdfs dfs -rm -r -f -skipTrash "$OUT" >/dev/null 2>&1

echo "=== EXP $EXP | mode=$MODE | input=$INPUT | out=$OUT | extra: $* ===" | tee "$LOG"
START=$(date +%s)
hadoop jar "$JAR" FailureSimulation "$@" -Dce06.simulation.mode="$MODE" "$MODE" "$INPUT" "$OUT" 2>&1 | tee -a "$LOG"
RC=${PIPESTATUS[0]}
END=$(date +%s)
echo "=== EXIT=$RC DURATION=$((END-START))s ===" | tee -a "$LOG"

# Extract job id (client side prints it; also capture from RM)
JOBID=$(grep -oE 'job_[0-9]+_[0-9]+' "$LOG" | tail -1)
APPID=$(grep -oE 'application_[0-9]+_[0-9]+' "$LOG" | tail -1)
echo "JOBID=${JOBID:-none} APPID=${APPID:-none}" | tee -a "$LOG"

if [ -n "${JOBID:-}" ] && [ "$JOBID" != "none" ]; then
  # Job history evidence: job status + task attempts (incl. failed/speculative)
  sleep 3
  {
    echo "########## JOB HISTORY EVIDENCE: $JOBID (exp $EXP) ##########"
    mapred job -history "$OUT" 2>&1 || true
    echo "----- yarn application -status -----"
    [ -n "${APPID:-}" ] && yarn application -status "$APPID" 2>&1 || true
    echo "----- CE06 counters (mapred job -counter) -----"
    mapred job -counter "$JOBID" 'CE06 Failure Simulation' 2>&1 || true
    echo "----- task completion events (mapred job -events? fallback: history via REST) -----"
    curl -s "http://localhost:19888/ws/v1/history/mapreduce/jobs/$JOBID" 2>/dev/null | head -c 4000 || true
    echo
  } > "$JHDIR/exp${EXP}_jobhistory_${JOBID}.txt" 2>&1
  # Task attempts detail (the core evidence: which attempts failed/killed/speculative)
  curl -s "http://localhost:19888/ws/v1/history/mapreduce/jobs/$JOBID/tasks" 2>/dev/null \
    > "$JHDIR/exp${EXP}_tasks_${JOBID}.json" || true
  echo "Evidence written: $JHDIR/exp${EXP}_*_${JOBID}.*" | tee -a "$LOG"
fi

# Save job output part file sample
hdfs dfs -cat "$OUT/part-r-*" 2>/dev/null | head -30 | tee -a "$LOG"
echo "RESULT=$([ $RC -eq 0 ] && echo SUCCESS || echo FAILED) EXP=$EXP JOBID=${JOBID:-none}" | tee -a "$LOG"
exit $RC
