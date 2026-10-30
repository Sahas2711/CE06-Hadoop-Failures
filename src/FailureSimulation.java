import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * CE-06 (Failures and Speculative Execution) - NASA HTTP log analyzer.
 *
 * Counts HTTP responses by status code from NASA combined-format logs and
 * provides five controlled failure/slowdown simulation modes.
 *
 * Modes (positional arg 1, also settable via -Dce06.simulation.mode):
 *   NORMAL                     process normally; broken/marker lines skipped + counted
 *   MALFORMED_RECORD_EXCEPTION throw IOException on every CE06_BAD_RECORD line:
 *                              every attempt touching it fails -> retries -> job fails
 *   FAIL_ONCE                  throw on CE06_BAD_RECORD only in attempt _0 of a task;
 *                              the retry (_1) skips the marker -> job succeeds
 *   ALWAYS_FAIL                throw in setup() for failing partitions (default 0):
 *                              every attempt fails instantly, regardless of input
 *   SLOW_TASK                  sleep on slow partitions (default 0) but ONLY in
 *                              attempt _0: creates a straggler; retries/speculative
 *                              duplicates run fast
 *
 * Job-level knobs passed as generic options:
 *   -Dce06.sleep.ms=150            sleep per 1000 lines in slow attempt (default 0)
 *   -Dce06.slow.taskpartitions=0   comma list of slow partitions
 *   -Dce06.failing.taskpartitions=0  comma list of ALWAYS_FAIL partitions
 *   -Dmapreduce.map.maxattempts=N  attempts allowed per map task
 *   -Dmapreduce.map.speculative=true|false
 *
 * Usage:
 *   hadoop jar FailureSimulation.jar FailureSimulation \
 *       [-D...] <MODE> <input> <output>
 */
public class FailureSimulation extends Configured implements Tool {

    public static final String BAD_MARKER = "CE06_BAD_RECORD";
    public static final String MODE_KEY = "ce06.simulation.mode";
    public static final String SLEEP_KEY = "ce06.sleep.ms";
    public static final String SLOW_PARTS_KEY = "ce06.slow.taskpartitions";
    public static final String FAIL_PARTS_KEY = "ce06.failing.taskpartitions";
    public static final String SLEEP_EVERY_KEY = "ce06.sleep.every";

    public static final String COUNTER_GROUP = "CE06 Failure Simulation";

    // NASA log tail:  " 200 6245   or   " 200 -
    private static final Pattern STATUS_TAIL =
            Pattern.compile("\"\\s+(\\d{3})\\s+(\\d+|-)\\s*$");

    private static boolean inList(String list, int id) {
        for (String s : list.split(",")) {
            if (s.trim().equals(String.valueOf(id))) {
                return true;
            }
        }
        return false;
    }

    public static class StatusMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private static final Text STATUS_OUT = new Text();
        private static final IntWritable ONE = new IntWritable(1);

        private String mode;
        private long sleepMs;
        private long sleepEvery;
        private boolean slowThisTask;
        private String attemptId;

        @Override
        protected void setup(Context ctx) throws IOException, InterruptedException {
            Configuration conf = ctx.getConfiguration();
            mode = conf.get(MODE_KEY, "NORMAL");
            sleepMs = conf.getLong(SLEEP_KEY, 0L);
            sleepEvery = conf.getLong(SLEEP_EVERY_KEY, 1000L);
            attemptId = ctx.getTaskAttemptID().toString();
            int part = ctx.getTaskAttemptID().getTaskID().getId();
            slowThisTask = inList(conf.get(SLOW_PARTS_KEY, "0"), part);

            if ("ALWAYS_FAIL".equals(mode)
                    && inList(conf.get(FAIL_PARTS_KEY, "0"), part)) {
                ctx.getCounter(COUNTER_GROUP, "ALWAYS_FAIL_ATTEMPT_THROWN").increment(1);
                throw new IOException(
                        "CE06 simulated failure: mode=ALWAYS_FAIL attempt="
                        + attemptId + " partition=" + part
                        + " (thrown in setup - every attempt fails)");
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context ctx)
                throws IOException, InterruptedException {

            String line = value.toString();
            Counter read = ctx.getCounter(COUNTER_GROUP, "LINES_READ");
            read.increment(1);

            // ---- injected malformed record handling ----
            if (line.contains(BAD_MARKER)) {
                ctx.getCounter(COUNTER_GROUP, "MALFORMED_MARKER_ENCOUNTERED").increment(1);
                switch (mode) {
                    case "MALFORMED_RECORD_EXCEPTION":
                        throw new IOException(
                                "CE06 simulated failure: mode=MALFORMED_RECORD_EXCEPTION"
                                + " attempt=" + attemptId
                                + " record="
                                + line.substring(0, Math.min(80, line.length())));
                    case "FAIL_ONCE":
                        if (attemptId.endsWith("_0")) {
                            throw new IOException(
                                    "CE06 simulated failure: mode=FAIL_ONCE first attempt"
                                    + " attempt=" + attemptId
                                    + " record="
                                    + line.substring(0, Math.min(80, line.length())));
                        }
                        ctx.getCounter(COUNTER_GROUP, "BAD_LINE_SKIPPED_ON_RETRY")
                           .increment(1);
                        return;
                    default:
                        // NORMAL / SLOW_TASK / ALWAYS_FAIL: skipped + counted, never silent
                        ctx.getCounter(COUNTER_GROUP, "BAD_LINE_SKIPPED").increment(1);
                        return;
                }
            }

            // ---- naturally malformed line (no marker, not parseable) ----
            Matcher m = STATUS_TAIL.matcher(line);
            if (!m.find()) {
                ctx.getCounter(COUNTER_GROUP, "UNPARSEABLE_SKIPPED").increment(1);
                return;
            }

            // ---- SLOW_TASK: straggler only in the first attempt (_0) ----
            if ("SLOW_TASK".equals(mode) && slowThisTask
                    && attemptId.endsWith("_0") && sleepMs > 0) {
                if ((read.getValue() % sleepEvery) == 0) {
                    Thread.sleep(sleepMs);
                }
            }

            STATUS_OUT.set(m.group(1));
            ctx.write(STATUS_OUT, ONE);
        }
    }

    public static class SumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context ctx)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v : values) {
                sum += v.get();
            }
            result.set(sum);
            ctx.write(key, result);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println(
                "Usage: FailureSimulation <MODE> <input> <output>\n"
                + "  MODE in {NORMAL, MALFORMED_RECORD_EXCEPTION, FAIL_ONCE,"
                + " ALWAYS_FAIL, SLOW_TASK}");
            return 2;
        }
        String mode = args[0];
        String input = args[1];
        String output = args[2];

        Configuration conf = getConf();
        conf.set(MODE_KEY, mode);

        Job job = Job.getInstance(conf, "CE06-" + mode + "-" + System.currentTimeMillis());
        job.setJarByClass(FailureSimulation.class);
        job.setMapperClass(StatusMapper.class);
        job.setReducerClass(SumReducer.class);
        job.setNumReduceTasks(1);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        boolean ok = job.waitForCompletion(true);

        // Effective values actually used by this job (proves config was read)
        System.out.println("CE06-EFFECTIVE-CONF mapreduce.map.maxattempts="
                + conf.get("mapreduce.map.maxattempts", "<default:4>")
                + " mapreduce.map.speculative="
                + conf.get("mapreduce.map.speculative", "<default:true>")
                + " mapreduce.reduce.speculative="
                + conf.get("mapreduce.reduce.speculative", "<default:true>")
                + " mapreduce.framework.name="
                + conf.get("mapreduce.framework.name", "<unset>"));

        org.apache.hadoop.mapreduce.Counters ctr = job.getCounters();
        java.util.Map<String, Long> vals = new java.util.LinkedHashMap<>();
        for (String cn : new String[] {
                "LINES_READ", "MALFORMED_MARKER_ENCOUNTERED", "BAD_LINE_SKIPPED",
                "BAD_LINE_SKIPPED_ON_RETRY", "UNPARSEABLE_SKIPPED",
                "ALWAYS_FAIL_ATTEMPT_THROWN"}) {
            vals.put(cn, ctr.findCounter(COUNTER_GROUP, cn).getValue());
        }
        StringBuilder sb = new StringBuilder();
        sb.append("CE06 COUNTERS mode=").append(mode)
          .append(" job=").append(job.getJobID());
        for (java.util.Map.Entry<String, Long> e : vals.entrySet()) {
            sb.append(' ').append(e.getKey()).append('=').append(e.getValue());
        }
        sb.append(" SUCCESS=").append(ok);
        System.out.println(sb.toString());
        return ok ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int rc = ToolRunner.run(new Configuration(), new FailureSimulation(), args);
        System.exit(rc);
    }
}
