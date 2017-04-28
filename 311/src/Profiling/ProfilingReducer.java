import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProfilingReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String csvColumn = key.toString();
        if (csvColumn.equals("incident_zip")) {
            int cnt = 0, min=Integer.MAX_VALUE, max=Integer.MIN_VALUE;
            for (Text val: values) {
                String value = val.toString().trim();
                if (value.equals("")||value == null || value.isEmpty()){ cnt++;continue;}
		else if(value.contains("\n")||value.contains("\"")||value.matches(".*[a-zA-Z]+.*")) continue;
		else{
		    if(value.contains("-")){
			String[] arr = value.split("-");
		        value = arr[0];
                    }
		    if(value.contains(" ")){
			value = value.replace(" ","");
		    }
		    if(value.contains("*")){
                        value = value.replace("*","");
                    }	
		    if (value.equals("")||value == null || value.isEmpty()){ cnt++;continue;}
                    min = Math.min(min, Integer.parseInt(value));
                    max = Math.max(max, Integer.parseInt(value));
                }
            }
            context.write(key, new Text("Null Count : " + cnt));
            context.write(key, new Text("String Length Range : " + min+"-"+max));	
        }  else if (csvColumn.equals("latitude+longitude")) {
            int cnt = 0;
            for (Text val: values) {
                cnt++;
            }
            context.write(key, new Text("Null Count : " + cnt));
        } else if (csvColumn.equals("agency")) {
            int cnt = 0,min=Integer.MAX_VALUE,max=Integer.MIN_VALUE;
            HashMap<String,Integer> uniqueCnt = new HashMap<>();
            for (Text val: values) {
                String value = val.toString();
                if (value == null || value.isEmpty()) cnt++;
                else {
                    if(uniqueCnt.get(value)!=null)
                        uniqueCnt.put(value, uniqueCnt.get(value)+1);
                    else
                        uniqueCnt.put(value,1);
                    min = Math.min(min, value.length());
                    max = Math.max(max, value.length());
                }
            }
            context.write(key, new Text("Null Count : " + cnt));
            context.write(key, new Text("String Length Range : " + min+"-"+max));
            for(String k :  uniqueCnt.keySet())
                context.write(key, new Text(k+" : " + uniqueCnt.get(k)));
        } else if (csvColumn.equals("complaint_type")) {
            HashMap<String,Integer> uniqueCnt = new HashMap<>();
            int count=0, min = Integer.MAX_VALUE, max = Integer.MIN_VALUE;
            for (Text val: values) {
                String value = val.toString();
                if (value == null || value.isEmpty()) count++;
                else {
                    if(uniqueCnt.get(value)!=null)
                        uniqueCnt.put(value, uniqueCnt.get(value)+1);
                    else
                        uniqueCnt.put(value,1);
                    min = Math.min(min, value.length());
                    max = Math.max(max, value.length());
                }
            }
            context.write(key, new Text("Null Count : " + count));
            context.write(key, new Text("String Length Range : " + min+"-"+max));
            for(String k :  uniqueCnt.keySet())
                context.write(key, new Text(k+" : " + uniqueCnt.get(k)));
        } else if (csvColumn.equals("location_type")) {
            int max = Integer.MIN_VALUE, count = 0;
            for (Text val: values) {
                String value = val.toString();
                if (value == null || value.isEmpty()) count++;
                else {
                    max = Math.max(max, value.length());
                }
            }
            context.write(key, new Text("Null Count : " + count));
            context.write(key, new Text("String Length Range : " + max));
        } else if (csvColumn.equals("facility_type")) {
            int max = Integer.MIN_VALUE, count = 0;
            for (Text val: values) {
                String value = val.toString();
                if (value == null || value.isEmpty()) count++;
                else {
                    max = Math.max(max, value.length());
                }
            }
            context.write(key, new Text("Null Count : " + count));
            context.write(key, new Text("String Length Range : " + max));
        }
    }
}
