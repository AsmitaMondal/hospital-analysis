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

public class ChronicLifestyleAnalysis {

    // Mapper Class
    public static class AnalysisMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text keyOut = new Text();
        private IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(","); // Assuming CSV format

            try {
                String outcome = fields[8].trim(); // OUTCOME column (index 8)
                String dm = fields[11].trim(); // DM column (index 11)
                String htn = fields[12].trim(); // HTN column (index 12)
                String smoking = fields[9].trim(); // SMOKING column (index 9)
                String alcohol = fields[10].trim(); // ALCOHOL column (index 10)

                // Chronic Conditions Analysis
                String chronicKey = "Chronic:" + (dm.equals("1") ? "DM_Yes" : "DM_No") + "_" +
                                    (htn.equals("1") ? "HTN_Yes" : "HTN_No") + ":" + outcome;
                keyOut.set(chronicKey);
                context.write(keyOut, one);

                // Lifestyle Analysis
                String lifestyleKey = "Lifestyle:" + (smoking.equals("1") ? "Smoking_Yes" : "Smoking_No") + "_" +
                                      (alcohol.equals("1") ? "Alcohol_Yes" : "Alcohol_No") + ":" + outcome;
                keyOut.set(lifestyleKey);
                context.write(keyOut, one);
            } catch (ArrayIndexOutOfBoundsException e) {
                // Ignore invalid rows
            }
        }
    }

    // Partitioner Class
    public static class AnalysisPartitioner extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            if (key.toString().startsWith("Chronic")) {
                return 0; // Chronic Conditions go to Partition 0
            } else if (key.toString().startsWith("Lifestyle")) {
                return 1; // Lifestyle Factors go to Partition 1
            }
            return 2; // Default
        }
    }

    // Reducer Class
    public static class AnalysisReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
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
        Job job = Job.getInstance(conf, "Chronic and Lifestyle Analysis");
        job.setJarByClass(ChronicLifestyleAnalysis.class);

        job.setMapperClass(AnalysisMapper.class);
        job.setPartitionerClass(AnalysisPartitioner.class);
        job.setReducerClass(AnalysisReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Set the number of reducers to 2 (one for chronic conditions, one for lifestyle factors)
        job.setNumReduceTasks(2);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
