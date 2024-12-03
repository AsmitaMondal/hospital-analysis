import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ICUStayDurationAnalysis {

    // Mapper Class
    public static class ICUStayMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        private Text outputKey = new Text();
        private DoubleWritable outputValue = new DoubleWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(","); // Assuming CSV format

            try {
                String admissionType = fields[4].trim();  // Admission Type column
                String treatmentType = fields[10].trim(); // Treatment Type column (example column index)
                String outcome = fields[8].trim(); // Outcome column
                String icuStayStr = fields[6].trim();  // ICU Stay Duration column

                double icuStay = Double.parseDouble(icuStayStr);

                // Emit for Admission Type
                outputKey.set("AdmissionType:" + admissionType);
                outputValue.set(icuStay);
                context.write(outputKey, outputValue);

                // Emit for Treatment Type
                outputKey.set("TreatmentType:" + treatmentType);
                context.write(outputKey, outputValue);

                // Emit for Outcome
                outputKey.set("Outcome:" + outcome);
                context.write(outputKey, outputValue);

            } catch (Exception e) {
                // Ignore invalid records
            }
        }
    }

    // Reducer Class
    public static class ICUStayReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;

            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }

            // Calculate the average ICU stay duration
            double average = sum / count;
            result.set(average);
            context.write(key, result);
        }
    }

    // Driver Class
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "ICU Stay Duration Analysis");
        job.setJarByClass(ICUStayDurationAnalysis.class);

        job.setMapperClass(ICUStayMapper.class);
        job.setReducerClass(ICUStayReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0])); // Input path
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // Output path

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
