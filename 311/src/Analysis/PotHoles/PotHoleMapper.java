import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class PotHoleMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {

        String[] data = value.toString().split(",");
        if(data.length>50)
        {

            if(data[6].toLowerCase().contains("pothole")){
                String zip = data[8];
                zip = zip.trim();
                if(!zip.equals("") && !zip.equals("0"))
                    output.collect(new Text(zip), new IntWritable(1));
            }
        }
        //Else Pass
    }
}
