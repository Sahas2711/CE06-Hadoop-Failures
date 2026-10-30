#!/bin/bash
# Helper: complete the hadoop distro copy using rsync (idempotent, skips existing)
# Safe to rerun. Excludes nothing - full integrity copy.
SRC=/mnt/c/bigdata/hadoop-3.4.2/
DST=/opt/hadoop-3.4.2/
echo "Starting rsync at $(date)..."
rsync -a "$SRC" "$DST"
RC=$?
echo "rsync exit code: $RC at $(date)"
echo "wsl_total_files=$(find /opt/hadoop-3.4.2 -type f | wc -l)"
echo "src_total_files=$(find /mnt/c/bigdata/hadoop-3.4.2 -type f | wc -l)"
exit $RC
