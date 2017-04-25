import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Created by anish on 4/25/17.
 */
public class AreaClassifierMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        Map<String, Integer> indexMap = new LinkedHashMap<>();
        indexMap.put("name", 25);
        indexMap.put("rating", 3);
        indexMap.put("price", 19);
        indexMap.put("parking", 6);
        indexMap.put("alcohol", 10);
        indexMap.put("payment_cashonly", 12);
        indexMap.put("latitude", 15);
        indexMap.put("longitude", 28);

        if (key.get() == 0) return;

        String line = value.toString();
        String[] vals = line.split("\t");

        if (vals[6] == null || vals[6].isEmpty()) {
            vals[6] = "False";
        }

        if (vals[10] == null || vals[10].isEmpty()) {
            vals[10] = "False";
        }

        if (vals[12] == null || vals[12].isEmpty()) {
            vals[12] = "False";
        }

        if (vals[3] == null || vals[3].isEmpty()) {
            vals[3] = "0";
        }

        if (vals[19] == null || vals[19].isEmpty()) {
            vals[19] = "0";
        }

//        If either of latitude or longitude is null, omit the row.
        if (vals[15] == null || vals[15].isEmpty() || vals[28] == null || vals[28].isEmpty()) {
            return;
        }

        String csvRow = "";
        String separator = "";
        for (String col: indexMap.keySet()) {
            csvRow += separator + vals[indexMap.get(col)];
            separator = ",";
        }

        context.write(key, new Text(csvRow));
    }
}
