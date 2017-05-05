import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Created by Naman on 4/21/17.
 */
public class DataCleanerMapper
        extends Mapper<LongWritable, Text, LongWritable, Text> {


        @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        if (key.get() != 0) return;
        
		int qIdx = line.indexOf('"');
        int eIdx = qIdx + 1;
        while(  qIdx != -1 && eIdx != line.length()-1 && line.charAt(eIdx)!= '"'){
            eIdx++;
        }
        if( qIdx != -1 && eIdx != line.length()-1){
			String substring = line.substring(qIdx,eIdx);
			String sbStr = substring.replaceAll(","," ");
			line = line.replace(substring,sbStr);
		}
        String[] vals = line.split(",");
		if(vals.length <23) return;
		
		
		Map<String, Integer> indexMap = new TreeMap<>();
        indexMap.put("Complaint_From_Date", 1);
        indexMap.put("Complaint_From_Time", 2);
		indexMap.put("Offence_Key", 6);
        indexMap.put("Crime_Key", 8);
        indexMap.put("Crime_Status", 10);
        indexMap.put("Category", 11);
		indexMap.put("Location", 15);
        indexMap.put("Premise_Type", 16);
        indexMap.put("Latitude", 21);
        indexMap.put("Longitude", 22);
		
		
		if (vals[1] == null || vals[1].isEmpty()) {
			vals[1] = "Unknown";
		}
		
		if (vals[2] == null || vals[2].isEmpty()) {
			vals[2] = "Unknown";
		}
		
		if (vals[6] == null || vals[6].isEmpty()) {
			vals[6] = "Unknown";
		}
		
		if (vals[8] == null || vals[8].isEmpty()) {
			vals[8] = "Unknown";
		}
		
		if (vals[10] == null || vals[10].isEmpty()) {
			vals[10] = "Unknown";
		}
		
		if (vals[11] == null || vals[11].isEmpty()) {
			vals[11] = "Unknown";
		}
		
		if (vals[15] == null || vals[15].isEmpty()) {
			vals[15] = "Unknown";
		}
		
		if (vals[16] == null || vals[16].isEmpty()) {
			vals[16] = "Unknown";
		}

		if (vals[21] == null || vals[21].isEmpty() || vals[22] == null || vals[22].isEmpty()) {
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
