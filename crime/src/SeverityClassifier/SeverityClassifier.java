import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

/**
 * Created by anish on 4/25/17.
 */
public class SeverityClassifier {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: SeverityClassifier <input path> <output path>");
            System.exit(-1);
        }

        Job job = new Job();
        job.setJarByClass(SeverityClassifier.class);
        job.setJobName("Severity Classification");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(SeverityClassifierMapper.class);
        job.setReducerClass(SeverityClassifierReducer.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.addCacheFile(new URI("project/input/kycd_values.txt"));
        job.addCacheFile(new URI("project/input/pdcd_values.txt"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
