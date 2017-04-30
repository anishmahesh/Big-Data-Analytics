import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Created by anish on 4/25/17.
 */
public class SeverityClassifierReducer extends Reducer<LongWritable, Text, Text, Text> {

    @Override
    public void reduce(LongWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        for (Text val: values) {
            context.write(null, val);
        }

    }
}
