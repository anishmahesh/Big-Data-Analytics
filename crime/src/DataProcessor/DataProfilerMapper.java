import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DataProfilerMapper
        extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
	if (line.length() > 60){
		int qIdx = line.indexOf('"');
		int eIdx = qIdx + 1;
		while(  eIdx != line.length() && line.charAt(eIdx)!= '"'){
		    eIdx++;
		}
		if(eIdx != line.length()){
			String substring = line.substring(qIdx,eIdx);
			String sbStr = substring.replaceAll(","," ");
			line = line.replace(substring,sbStr);
		}	
		String[] vals = line.split(",");
		if(key.get() !=0 && vals.length >21){
			if (vals[15] == null || vals[15].isEmpty()) {
			    context.write(new Text("Location"), new Text("Other"));
			} else {
			    context.write(new Text("Location"),new Text(vals[15]));
			}


			if (vals[10] == null || vals[10].isEmpty()) {
			    context.write(new Text("Crime_Status"), new Text("Other"));
			} else {
			    context.write(new Text("Crime_Status"), new Text(vals[10]));
			}


			if (vals[12] == null || vals[12].isEmpty()) {
			    context.write(new Text("Premise_Type"), new Text("Crime_premise"));
			} else {
			    context.write(new Text("Premise_Type"), new Text(String.valueOf(vals[12].length())));
			}


			if (vals[21] == null || vals[21].isEmpty() || vals[22] == null || vals[22].isEmpty()) {
			    context.write(new Text("latitude+longitude"), new Text("1"));
			}

			context.write(new Text("Offence_Key"), new Text(vals[6]));

			context.write(new Text("Crime_Key"), new Text(vals[8]));

			context.write(new Text("Category"), new Text(String.valueOf(vals[11].length())));
		}
        }
    }
}
