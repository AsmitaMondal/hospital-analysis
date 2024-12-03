import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HospitalAnalysis {

    // Mapper Class
    public static class StatisticsMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");

            if (fields.length < 25 || fields[0].equals("MRD No.")) {
                // Skip invalid rows or header row
                return;
            }

            // Row Count
            context.write(new Text("RowCount"), one);

            // Type of Admission
            if (!fields[4].isEmpty()) {
                context.write(new Text("Type_" + fields[4]), one);
            }

            // Outcome Distribution
            if (!fields[8].isEmpty()) {
                context.write(new Text("Outcome_" + fields[8]), one);
            }

            // Smoking and Alcohol
            context.write(new Text("Smoking_" + (fields[9].equals("1") ? "Yes" : "No")), one);
            context.write(new Text("Alcohol_" + (fields[10].equals("1") ? "Yes" : "No")), one);

            // Diabetes (DM), Hypertension (HTN), CKD
            context.write(new Text("DM_" + (fields[11].equals("1") ? "Yes" : "No")), one);
            context.write(new Text("HTN_" + (fields[12].equals("1") ? "Yes" : "No")), one);
            context.write(new Text("CKD_" + (fields[15].equals("1") ? "Yes" : "No")), one);

            // Severe Anemia
            if (fields[24].equals("1")) {
                context.write(new Text("SevereAnemia_Count"), one);
            }

            // ICU Stay Distribution
            try {
                int icuDuration = Integer.parseInt(fields[7]);
                if (icuDuration < 1) {
                    context.write(new Text("ICUStay_<1"), one);
                } else if (icuDuration <= 3) {
                    context.write(new Text("ICUStay_1-3"), one);
                } else {
                    context.write(new Text("ICUStay_>3"), one);
                }
            } catch (NumberFormatException e) {
                // Ignore invalid ICU duration values
            }

            // Age Group Distribution
            try {
                int age = Integer.parseInt(fields[1]);
                if (age < 18) {
                    context.write(new Text("Age_<18"), one);
                } else if (age <= 35) {
                    context.write(new Text("Age_18-35"), one);
                } else if (age <= 60) {
                    context.write(new Text("Age_36-60"), one);
                } else {
                    context.write(new Text("Age_>60"), one);
                }
            } catch (NumberFormatException e) {
                // Ignore invalid age values
            }

            // Monthly Admission Trends
            if (!fields[5].isEmpty()) {
                context.write(new Text("Month_" + fields[5]), one);
            }

            // Rural vs. Urban Admissions
            context.write(new Text(fields[3].equals("R") ? "Rural" : "Urban"), one);
        }
    }

    // Reducer Class
    public static class StatisticsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    // Driver Class
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Hospital Statistics");

        job.setJarByClass(HospitalAnalysis.class);
        job.setMapperClass(StatisticsMapper.class);
        job.setCombinerClass(StatisticsReducer.class); // Optional Combiner
        job.setReducerClass(StatisticsReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0])); // Input path
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // Output path

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
