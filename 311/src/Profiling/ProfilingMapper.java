import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProfilingMapper extends Mapper<LongWritable, Text, Text, Text> {
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        String inpLine = value.toString();
        String[] values = inpLine.split(",");
        
        context.write(new Text("agency"), new Text(values[3]));
        context.write(new Text("complaint_type"), new Text(values[5]));
        context.write(new Text("location_type"), new Text(values[8]));
        context.write(new Text("facility_type"), new Text(values[18]));
        context.write(new Text("incident_zip"), new Text(values[8]));
        
        if(values.length>51){
            if (values[50] == null || values[50].isEmpty() || values[51] == null || values[51].isEmpty()) {
                context.write(new Text("latitude+longitude"), new Text("1"));
            }
        }

    }
}

