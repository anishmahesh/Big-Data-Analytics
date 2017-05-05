/**
 * Created by RohitMuthyala on 21/04/17.
 */
 import java.io.IOException;
 import java.util.Map;
 import java.util.TreeMap;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DataCleaningReducer extends Reducer<LongWritable, Text, Text, Text> {

    @Override
    public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text val: values) {
            context.write(null, val);
        }

    }
}
