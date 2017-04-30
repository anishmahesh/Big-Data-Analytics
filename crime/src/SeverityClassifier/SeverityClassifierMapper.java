import java.io.*;
import java.util.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.filecache.DistributedCache;

/**
 * Created by anish on 4/25/17.
 */
public class SeverityClassifierMapper extends Mapper<LongWritable, Text, LongWritable, Text> {


    HashMap<Integer,Integer> public_key_to_val = new HashMap<>();
    HashMap<Integer,Integer> police_key_to_val = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        super.setup(context);

        if (context.getCacheFiles() != null
                && context.getCacheFiles().length > 0) {
            BufferedReader br = new BufferedReader(new FileReader(new File("./kycd_values.txt")));
            createDictionaryFromFile(br, public_key_to_val);

            BufferedReader br1 = new BufferedReader(new FileReader(new File("./pdcd_values.txt")));
            createDictionaryFromFile(br1, police_key_to_val);
        }
        super.setup(context);
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        HashSet<Integer> publicSeverityOne = new HashSet<>();
        HashSet<Integer> publicSeverityTwo = new HashSet<>();
        int publicStartIdx = 0;
        int publicEndIdx = 71;

        HashSet<Integer> policeSeverityOne = new HashSet<>();
        HashSet<Integer> policeSeverityTwo = new HashSet<>();
        int policeStartIdx = 72;
        int policeEndIdx = 463;

        if(key.get() == 0){
            String line = value.toString();
            String[] vals = line.split(",");
            addIndexesToSeverity(publicStartIdx, publicEndIdx, publicSeverityOne, publicSeverityTwo, vals, public_key_to_val);
            addIndexesToSeverity(policeStartIdx, policeEndIdx, policeSeverityOne, policeSeverityTwo, vals, police_key_to_val);
        } else {
            StringBuilder sb = new StringBuilder("");
            String line = value.toString();
            String[] vals = line.split(",");
            int intVals = -1;
            for(int i=publicStartIdx; i<=publicEndIdx; i++){
                vals[i] = processString(vals[i]);
                intVals = Integer.parseInt(vals[i]);
                if(intVals == 1){
                    if(publicSeverityTwo.contains(intVals)){
                        sb.append("0");
                        sb.append("1");
                    } else {
                        sb.append("1");
                        sb.append("0");
                    }
                    break;
                }
            }
            if(intVals == -1){
                sb.append("0");
                sb.append("1");
            }

            intVals = -1;
            for(int i=policeStartIdx; i<=policeEndIdx; i++){
                vals[i] = processString(vals[i]);
                intVals = Integer.parseInt(vals[i]);
                if(intVals == 1){
                    if(policeSeverityTwo.contains(intVals)){
                        sb.append("0");
                        sb.append("1");
                    } else {
                        sb.append("1");
                        sb.append("0");
                    }
                    break;
                }
            }

            if(intVals == -1){
                sb.append("0");
                sb.append("1");
            }

            for(int i = policeEndIdx+1; i < vals.length; i++){
                sb.append(vals[i]);
            }
            String csvRow = sb.toString();

            context.write(key, new Text(csvRow));
        }
    }

    public void addIndexesToSeverity(int start,int end,HashSet<Integer> severityOne,HashSet<Integer> severityTwo, String[] vals, HashMap<Integer,Integer> key_to_val){
        for(int i = start; i <= end; i++){
            vals[i] = processString(vals[i]);
            int intVals = Integer.parseInt(vals[i]);
            if(key_to_val.containsKey(intVals)){
                if(key_to_val.get(intVals) >=3){
                    severityTwo.add(intVals);
                } else {
                    severityOne.add(intVals);
                }
            }
        }
    }

    public void createDictionaryFromFile(BufferedReader br, HashMap<Integer,Integer> key_to_val) throws IOException, InterruptedException{
        String line;
        while ((line = br.readLine()) != null) {
            String[] words = line.split(",");
            key_to_val.put(Integer.parseInt(words[0]), Integer.parseInt(words[2]));
        }
    }

    public void processString(String s){
        return s.replace(" ","");
    }

}
