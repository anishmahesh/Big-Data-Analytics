import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class DataProfilerReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        String col = key.toString();
        if (col.equals("trip_distance")) {
            Double min = 1000.0, max = 0.0;
            for (Text val: values) {
                String value = val.toString();
                min = Math.min(min, Double.parseDouble(value));
                max = Math.max(max, Double.parseDouble(value));
            }
            context.write(key, new Text("The range is: " + min+" - "+max));
        }

        else if (col.equals("pickup_latitude_longitude")) {
              int count = 0;
              for (Text val: values) {
                  count++;
              }
              context.write(key, new Text("Wrong entry count is: " + count));
          }

        else if (col.equals("dropoff_latitude_longitude")) {
              int count = 0;
              for (Text val: values) {
                  count++;
              }
              context.write(key, new Text("Wrong entry count is: " + count));
          }

        else if (col.equals("total_amount")) {
            double min = 1000.0, max = 0.0;
            for (Text val: values) {
                String value = val.toString();
                min = Math.min(min, Double.parseDouble(value));
                max = Math.max(max, Double.parseDouble(value));
          }
            context.write(key, new Text("The range is: " + min+" - "+max));
        }
    }
}
