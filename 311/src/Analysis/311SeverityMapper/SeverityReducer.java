import java.util.*;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Created by sanchitmehta on 4/29/17.
 */
public class SeverityReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
	
	int[] combinedRatings = new int[]{0,0,0,0};
        for (Text val: values) {
	     String[] vals = val.toString().split(",");
             for(int i=0;i<4;i++){
	          combinedRatings[i]+=Integer.parseInt(vals[i].trim());
	     }
            //context.write(null, val);
        }
	String csvRow = Arrays.toString(combinedRatings);
        csvRow = csvRow.substring(1,csvRow.length()-1);
	context.write(null,new Text(key.toString()+","+csvRow));
    }
}
