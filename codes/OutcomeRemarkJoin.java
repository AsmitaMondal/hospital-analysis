import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class OutcomeRemarkJoin {

    // Mapper for Hospital Data
    public static class HospitalMapper extends Mapper<Object, Text, Text, Text> {
        private Text outcomeKey = new Text();
        private Text hospitalData = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            
            // Skip header and ensure minimum column count
            if (fields.length < 9 || fields[0].equals("MRD No.")) {
                return;
            }

            String outcome = fields[8].trim(); // OUTCOME column

            outcomeKey.set(outcome);
            hospitalData.set("HOSPITAL");  // Indicate that this is hospital data

            context.write(outcomeKey, hospitalData);
        }
    }

    // Mapper for Remarks Data
    public static class RemarksMapper extends Mapper<Object, Text, Text, Text> {
        private Text outcomeKey = new Text();
        private Text remarkData = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            
            // Skip header and ensure minimum column count
            if (fields.length < 2 || fields[0].equals("Outcome")) {
                return;
            }

            String outcome = fields[0].trim(); // OUTCOME column
            String remark = fields[1].trim();  // Remark column

            outcomeKey.set(outcome);
            remarkData.set("REMARK," + remark);  // Prefix remark data to differentiate from hospital data

            context.write(outcomeKey, remarkData);
        }
    }

    // Reducer Class for Joining Data
    public static class OutcomeRemarkReducer extends Reducer<Text, Text, Text, Text> {
        private Text outputValue = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> remarks = new ArrayList<>();
            int outcomeCount = 0;

            // Process the values for the given outcome key
            for (Text value : values) {
                String valueString = value.toString();

                if (valueString.equals("HOSPITAL")) {
                    // Count hospital records for the outcome
                    outcomeCount++;
                } else if (valueString.startsWith("REMARK")) {
                    String remark = valueString.substring(7);  // Extract the remark part after "REMARK,"
                    remarks.add(remark);  // Add the remark to the list
                }
            }

            // Output the result for each outcome
            for (String remark : remarks) {
                String formattedOutput = String.format("%s, %d, %s", 
                    key.toString(),  // Outcome
                    outcomeCount,    // Outcome Count
                    remark           // Remark
                );
                
                outputValue.set(formattedOutput);
                context.write(new Text(""), outputValue);  // Emit the result
            }
        }
    }

    // Driver Class for Running the Job
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: OutcomeRemarkJoin <hospital_input_path> <remarks_input_path> <output_path>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Outcome and Remark Join");
        job.setJarByClass(OutcomeRemarkJoin.class);

        // Use MultipleInputs to handle different input file formats
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, HospitalMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RemarksMapper.class);
        
        job.setReducerClass(OutcomeRemarkReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
