import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DataProfilerMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
        String line = value.toString();
        String[] vals = line.split(",");
        if(vals[4].equals("trip_distance") || (Double.parseDouble(vals[4]) < 0)|| (Double.parseDouble(vals[4]) > 1000)){}
        else
            context.write(new Text("trip_distance"), new Text(vals[4]));
        if(vals[5].length() == 1)
            context.write(new Text("pickup_latitude_longitude"), new Text("1"));
        if(vals[9].length() == 1)
            context.write(new Text("dropoff_latitude_longitude"), new Text("1"));
        if(vals[17].equals("total_amount")|| (Double.parseDouble(vals[17]) < 0)|| (Double.parseDouble(vals[17]) > 1000)){}
        else
            context.write(new Text("total_amount"), new Text(vals[17]));
    }
}
