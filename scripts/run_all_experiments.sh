#!/bin/bash
# =============================================================================
# CE-06: Run ALL experiments 1-5 (and optional 6-7 wrappers are separate).
# Produces evidence under CE06_Hadoop_Practical/evidence/
# =============================================================================
set -u
PROJ=/mnt/d/sem7/BDA-assignment/CE06_Hadoop_Practical
RUN=$PROJ/scripts/run_experiment.sh
CLEAN=/data/clean/nasa_access_clean.log
MAL=/data/malformed/nasa_access_malformed.log

echo "##### CE06 EXPERIMENT SUITE START $(date) #####"

# --- EXP 1: NORMAL baseline (clean input) -> must SUCCEED ---------------
bash "$RUN" 01 /results/exp01_baseline NORMAL "$CLEAN"
echo; echo ">>> EXP1 done (expect SUCCESS)"; echo

# --- EXP 2a: MALFORMED_RECORD_EXCEPTION, maxattempts=2 -> must FAIL ------
bash "$RUN" 02a /results/exp02a_malformed_fail MALFORMED_RECORD_EXCEPTION "$MAL" \
     -Dmapreduce.map.maxattempts=2
echo; echo ">>> EXP2a done (expect FAILED after 2 attempts/task)"; echo

# --- EXP 2b: FAIL_ONCE (retry succeeds) -> job SUCCEEDS ------------------
bash "$RUN" 02b /results/exp02b_fail_once FAIL_ONCE "$MAL"
echo; echo ">>> EXP2b done (attempt _0 failed, _1 succeeded, job SUCCESS)"; echo

# --- EXP 3A: ALWAYS_FAIL maxattempts=2 on clean input -> FAIL fast -------
bash "$RUN" 03a /results/exp03a_attempts2 ALWAYS_FAIL "$CLEAN" \
     -Dmapreduce.map.maxattempts=2
echo; echo ">>> EXP3a done (expect FAILED, 2 attempts on failing task)"; echo

# --- EXP 3B: ALWAYS_FAIL maxattempts=4 on clean input -> FAIL ------------
bash "$RUN" 03b /results/exp03b_attempts4 ALWAYS_FAIL "$CLEAN" \
     -Dmapreduce.map.maxattempts=4
echo; echo ">>> EXP3b done (expect FAILED, 4 attempts on failing task)"; echo

# --- EXP 4: SLOW_TASK, speculative OFF -> one long-running attempt -------
bash "$RUN" 04 /results/exp04_spec_off SLOW_TASK "$CLEAN" \
     -Dce06.sleep.ms=150 -Dmapreduce.map.speculative=false \
     -Dmapreduce.reduce.speculative=false
echo; echo ">>> EXP4 done (spec OFF: task 000000 has exactly 1 attempt)"; echo

# --- EXP 5: SLOW_TASK, speculative ON -> duplicate attempt launched ------
bash "$RUN" 05 /results/exp05_spec_on SLOW_TASK "$CLEAN" \
     -Dce06.sleep.ms=150 -Dmapreduce.map.speculative=true \
     -Dmapreduce.job.speculative.speculativeCap=0.5
echo; echo ">>> EXP5 done (spec ON: task 000000 has 2 attempts)"; echo

echo "##### CE06 EXPERIMENTS 1-5 FINISHED $(date) #####"
echo "Summary (RESULT lines):"
grep -h "^RESULT=" $PROJ/evidence/logs/*.log 2>/dev/null || true
