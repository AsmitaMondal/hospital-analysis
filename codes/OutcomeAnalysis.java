import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class OutcomeAnalysis {

    // Mapper Class
    public static class OutcomeMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text outcomeKey = new Text();
        private IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(","); // Assuming CSV format

            try {
                String ageStr = fields[1].trim(); // AGE column (index 1)
                String gender = fields[2].trim(); // GENDER column (index 2)
                String outcome = fields[8].trim(); // OUTCOME column (index 8)

                int age = Integer.parseInt(ageStr);
                String ageCategory;

                // Categorize the age
                if (age < 18) {
                    ageCategory = "<18";
                } else if (age <= 35) {
                    ageCategory = "18-35";
                } else if (age <= 60) {
                    ageCategory = "36-60";
                } else {
                    ageCategory = ">60";
                }

                // Key format: Outcome:AgeCategory,Gender
                outcomeKey.set(outcome + ":" + ageCategory + "," + gender);
                context.write(outcomeKey, one);
            } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
                // Ignore invalid rows
            }
        }
    }

    // Partitioner Class
    public static class OutcomePartitioner extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            // Extract the outcome from the key to decide the partition
            String outcome = key.toString().split(":")[0]; // Extract outcome from key
            
            // Partition based on the outcome
            if (outcome.equals("DAMA")) {
                return 0; // Partition 0 for DAMA outcomes
            } else if (outcome.equals("DISCHARGE")) {
                return 1; // Partition 1 for DISCHARGE outcomes
            } else if (outcome.equals("EXPIRY")) {
                return 2; // Partition 2 for EXPIRY outcomes
            } else {
                return 3; // Default partition for any other unexpected outcome
            }
        }
    }

    // Reducer Class
    public static class OutcomeReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    // Driver Class
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Outcome Analysis by Demographics");
        job.setJarByClass(OutcomeAnalysis.class);

        job.setMapperClass(OutcomeMapper.class);
        job.setPartitionerClass(OutcomePartitioner.class);
        job.setReducerClass(OutcomeReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Specify input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Set the number of reducers to 3 to separate the outcomes (DAMA, DISCHARGE, EXPIRY)
        job.setNumReduceTasks(3);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
