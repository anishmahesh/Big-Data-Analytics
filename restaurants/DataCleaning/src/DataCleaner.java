import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by anish on 4/21/17.
 */
public class DataCleaner {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: DataCleaner <input path> <output path>");
            System.exit(-1);
        }

        Job job = new Job();
        job.setJarByClass(DataCleaner.class);
        job.setJobName("Data Cleaning");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(DataCleanerMapper.class);
        job.setReducerClass(DataCleanerReducer.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
