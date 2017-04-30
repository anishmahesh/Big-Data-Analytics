import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

/**
 * Created by sanchitmehta on 4/29/17.
 */

public class Severity {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: SeverityMapper <input_path> <output_path>");
            System.exit(-1);
        }

        Job job = new Job();
        job.setJarByClass(Severity.class);
        job.setJobName("Severity Classifier");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(SeverityMapper.class);
        job.setReducerClass(SeverityReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.addCacheFile(new URI("scores/AgencyRatings.csv"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
