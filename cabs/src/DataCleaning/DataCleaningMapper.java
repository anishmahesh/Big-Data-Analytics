import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DataCleaningMapper
        extends Mapper<LongWritable, Text, LongWritable, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        Map<String, Integer> indexMap = new TreeMap<>();
        indexMap.put("trip_distance", 4);
        indexMap.put("pickup_latitude_longitude", 5);
        indexMap.put("dropoff_latitude_longitude", 9);
        indexMap.put("total_amount", 17);

        if (key.get() == 0) return;

        String line = value.toString();
        String[] vals = line.split(",");

        if (vals[4] == null || vals[4].isEmpty()) {
            vals[4] = "0";
        }

        if (vals[17] == null || vals[17].isEmpty()) {
            vals[17] = "0";
        }
        if (vals[5] == null || vals[5].isEmpty() || vals[9] == null || vals[9].isEmpty()) {
            return;
        }

        String csvRow = ",";
        String separator = "";
        for (String col: indexMap.keySet()) {
            csvRow += separator + vals[indexMap.get(col)];
            separator = ",";
        }

        context.write(key, new Text(csvRow));
    }
}
