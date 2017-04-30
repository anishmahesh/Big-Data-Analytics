import java.io.*;
import java.util.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.filecache.DistributedCache;

/**
 * Created by sanchitmehta on 4/29/17.
 */
public class SeverityMapper extends Mapper<LongWritable, Text, Text, Text> {
    
    Map<String,Integer> agencyRatings = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException{
	super.setup(context);
        if (context.getCacheFiles() != null
                && context.getCacheFiles().length > 0) {

            BufferedReader br = new BufferedReader(new FileReader(new File("./AgencyRatings.csv")));
            String line;
            while ((line = br.readLine()) != null) {
                String[] words = line.split(",");
                agencyRatings.put(words[0], Integer.parseInt(words[1]));
            }

        }
        super.setup(context);
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {


        String line = value.toString();
        String[] vals = line.split(","); 
            int[] ratings = new int[4];
		
            if(agencyRatings.get(vals[1].trim())!=null){	
            	int rowRating = agencyRatings.get(vals[1].trim());  
            	ratings[rowRating-1] = 1;
	    }
	    //String csvRow = String.join(",", ratings);
	    String csvRow = Arrays.toString(ratings);
	    csvRow = csvRow.substring(1,csvRow.length()-1);

            context.write(new Text(vals[0]), new Text(csvRow));
    }

}
