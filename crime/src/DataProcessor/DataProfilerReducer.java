import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.*;

public class DataProfilerReducer
        extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        String col = key.toString();

        if (col.equals("Location")) {

            int count = 0;
			HashSet<String> uni = new HashSet<>();
            for (Text val: values) {
				String value = val.toString();
				if(value.equals("Other"))	count++;
                uni.add(value);
            }

            context.write(key, new Text("The null count is: " + count));
 	    context.write(key, new Text("Unique values are: " + uni.size()));

        } else if (col.equals("Crime_Status")) {
            int count = 0;
	    HashSet<String> uni = new HashSet<>();
            for (Text val: values) {
				String value = val.toString();
				if(value.equals("Other"))	count++;
				uni.add(value);
            }

            context.write(key, new Text("The null count is: " + count));
	    context.write(key, new Text("Unique values are: " + uni.size()));

        } else if (col.equals("Premise_Type")) {
            int count = 0;
            int min = Integer.MAX_VALUE, max = Integer.MIN_VALUE;
	    HashSet<String> uni = new HashSet<>();
            for (Text val: values) {
				String value = val.toString();
				if(!value.equals("Crime_premise")) {
					count++;
					min = Math.min(min, Integer.parseInt(value));
					max = Math.max(max, Integer.parseInt(value));
					uni.add(value);
				}
            }

            context.write(key, new Text("The non-null count is: " + count));
	    context.write(key, new Text("The range is: " + min+"-"+max));
	    context.write(key, new Text("Unique values are: " + uni.size()));

        } else if (col.equals("latitude+longitude")) {
            int count = 0;
            for (Text val: values) {
                count++;
            }

            context.write(key, new Text("The null count is: " + count));

        } else if (col.equals("Offence_Key")) {
            int min = Integer.MAX_VALUE, max = Integer.MIN_VALUE, count = 0;
	    HashSet<String> uni = new HashSet<>();
            for (Text val: values) {
                String value = val.toString();
                if (value == null || value.isEmpty()) count++;
                else {
                    min = Math.min(min, Integer.parseInt(value));
                    max = Math.max(max, Integer.parseInt(value));
					uni.add(value);
                }
            }

            context.write(key, new Text("The null count is: " + count));
            context.write(key, new Text("The range is: " + min+"-"+max));
	    context.write(key, new Text("Unique values are: " + uni.size()));

        } else if (col.equals("Crime_Key")) {

            int min = Integer.MAX_VALUE, max = Integer.MIN_VALUE, count = 0;
			HashSet<String> uni = new HashSet<>();
            for (Text val: values) {
                String value = val.toString();
                if (value == null || value.isEmpty()) count++;
                else {
                    min = Math.min(min, Integer.parseInt(value));
                    max = Math.max(max, Integer.parseInt(value));
					uni.add(value);
                }
            }

            context.write(key, new Text("The null count is: " + count));
            context.write(key, new Text("The range is: " + min+"-"+max));
	    context.write(key, new Text("Unique values are: " + uni.size()));

        } else if (col.equals("Category")) {

            int max = Integer.MIN_VALUE, count = 0;
            for (Text val: values) {
                String value = val.toString();
                if (value == null || value.isEmpty()) count++;
                else {
                    max = Math.max(max, Integer.parseInt(value));
                }
            }

            context.write(key, new Text("The null count is: " + count));
            context.write(key, new Text("The max length string is: " + max));

        }
    }
}
