import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class AverageDurationOfStay {

    // Mapper Class
    public static class DoSMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static Text DOS_KEY = new Text("DoS");
        private IntWritable duration = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(","); // Assuming CSV format
            try {
                // Extract DURATION OF STAY (assuming column index 6, adjust as needed)
                int stayDuration = Integer.parseInt(fields[6].trim());
                duration.set(stayDuration);
                context.write(DOS_KEY, duration);
            } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
                // Ignore invalid or missing rows
            }
        }
    }

    // Reducer Class
    public static class DoSReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int totalDuration = 0;
            int count = 0;

            for (IntWritable val : values) {
                totalDuration += val.get();
                count++;
            }

            double averageDuration = (count == 0) ? 0 : (double) totalDuration / count;
            result.set(averageDuration);
            context.write(key, result);
        }
    }

    // Driver Class
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Average Duration of Stay");
        job.setJarByClass(AverageDurationOfStay.class);
        job.setMapperClass(DoSMapper.class);
        job.setReducerClass(DoSReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
