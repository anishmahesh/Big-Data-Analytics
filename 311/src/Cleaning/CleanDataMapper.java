import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanDataMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        //LinkedHash Map to ensure the order of the csv values
        Map<String, Integer> keyMap = new TreeMap<>();
        keyMap.put("agency", 3);
        keyMap.put("complaint_type", 5);
        keyMap.put("location_type", 7);
        keyMap.put("Incident Zip", 8);
        keyMap.put("facility_type", 18);
        keyMap.put("latitude", 50);
        keyMap.put("longitude", 51);

        if (key.get() == 0) return;

        String line = value.toString();
        String[] vals = line.split(",");

        //To remove some corrupt records which do not have enough value
	if(vals.length<52)
	    return;

        if (vals[6] == null || vals[6].isEmpty()) {
            vals[6] = "N/A";
        }

        if (vals[5] == null || vals[5].isEmpty()) {
            vals[5] = "N/A";
        }

        if (vals[7] == null || vals[7].isEmpty()) {
            vals[7] = "N/A";
        }

        if (vals[18] == null || vals[18].isEmpty()) {
            vals[18] = "N/A";
        }
        
        String valueZip = vals[8];
        if (valueZip.equals("")||valueZip == null || valueZip.isEmpty()){return;}
        else if(valueZip.contains("NEWARK")||valueZip.contains("\n")||valueZip.contains("\"")||valueZip.matches(".*[a-zA-Z]+.*")||valueZip.contains("Lack")||valueZip.contains("NY")||valueZip.contains("School")||valueZip.contains("X")||valueZip.contains("Zip")||valueZip.contains("Flushometer")||valueZip.contains("Unclean")||valueZip.contains("Litter")||valueZip.contains("NA")||valueZip.contains("N/A")||valueZip.contains("Home")||valueZip.contains("Sidewalk")||valueZip.contains("Street")||valueZip.contains("Stop")) return;
        else{
            if(valueZip.contains("-")){
                String[] arr = valueZip.split("-");
                valueZip = arr[0];
            }
            if(valueZip.contains(" ")){
                valueZip = valueZip.replace(" ","");
            }
            if(valueZip.contains("*")){
                valueZip = valueZip.replace("*","");
            }
            if (valueZip.equals("")||value == null || valueZip.isEmpty()){ return;}
            if(Integer.parseInt(valueZip)>10001&&Integer.parseInt(valueZip)<14975)
                vals[8] = valueZip;
            else
                return;
        }
	//If either of latitude or longitude is null, omit the row.
        
	if (vals[50] == null || vals[50].isEmpty() || vals[50] == null || vals[50].isEmpty()|| vals[8].isEmpty()|| vals[8] == null) {
            return;
        }

        String csvRow = "";
        String separator = "";
        for (String col: keyMap.keySet()) {
            csvRow += separator + vals[keyMap.get(col)];
            separator = ",";
        }
        context.write(key, new Text(csvRow));
    }
}

