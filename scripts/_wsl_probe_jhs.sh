#!/bin/bash
J=job_1793346852065_0001
T=task_1793346852065_0001_m_000010
B="http://localhost:19888/ws/v1/history/mapreduce/jobs"
probe() {
  code=$(curl -s -o /tmp/probe.json -w '%{http_code}' "$1")
  echo "== $code  $1"
  head -c 300 /tmp/probe.json; echo; echo
}
probe "$B/$J/tasks/$T"
probe "$B/$J/tasks/$T/taskattempts"
probe "$B/$J/jobattempts"
probe "$B/$J/taskattempts"
probe "$B/$J/counters"
probe "$B/$J"
# UI task page (HTML) may embed attempts
probe "http://localhost:19888/jobhistory/taskattempt/job/$J/task_1793346852065_0001_m_000010"
