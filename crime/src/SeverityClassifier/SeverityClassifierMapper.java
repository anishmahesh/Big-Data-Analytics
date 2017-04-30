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
    HashSet<Integer> publicSeverityOne = new HashSet<>();
    HashSet<Integer> publicSeverityTwo = new HashSet<>();
    int publicStartIdx = 0;
    int publicEndIdx = 71;

    HashSet<Integer> policeSeverityOne = new HashSet<>();
    HashSet<Integer> policeSeverityTwo = new HashSet<>();
    int policeStartIdx = 72;
    int policeEndIdx = 463;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        super.setup(context);

        if (context.getCacheFiles() != null
                && context.getCacheFiles().length > 0) {
            //BufferedReader br = new BufferedReader(new FileReader(new File("./kycd_values.txt")));
            //createDictionaryFromFile(br, public_key_to_val);

            BufferedReader br1 = new BufferedReader(new FileReader(new File("./pdcd_values.txt")));
            createDictionaryFromFile(br1, police_key_to_val);

            BufferedReader br3 = new BufferedReader(new FileReader(new File("./Column_Header.csv")));
            addIndexesToSeverity(br3,publicStartIdx, publicEndIdx, publicSeverityOne, publicSeverityTwo, public_key_to_val);

            BufferedReader br2 = new BufferedReader(new FileReader(new File("./Column_Header.csv")));
            addIndexesToSeverity(br2,policeStartIdx, policeEndIdx, policeSeverityOne, policeSeverityTwo, police_key_to_val);
        }
        super.setup(context);
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        if(key.get() == 0) return;

        StringBuilder sb = new StringBuilder("");
        String line = value.toString();
        String[] vals = line.split(",");
        /*
        int intVals = 0;
        for(int i=publicStartIdx; i<=publicEndIdx; i++){
            vals[i] = processString(vals[i]);
            intVals = Integer.parseInt(vals[i]);
            if(intVals == 1){
                if(publicSeverityTwo.contains(i)){
                    sb.append("0,");
                    sb.append("1,");
                } else {
                    sb.append("1,");
                    sb.append("0,");
                }
                break;
            }
        }
        if(intVals != 1){
            sb.append("1,");
            sb.append("0,");
        }


        intVals = -1;
        for(int i=policeStartIdx; i<=policeEndIdx; i++){
            vals[i] = processString(vals[i]);
            intVals = Integer.parseInt(vals[i]);
            if(intVals == 1){
                if(policeSeverityTwo.contains(i)){
                    sb.append("0,");
                    sb.append("1");
                } else {
                    sb.append("1,");
                    sb.append("0");
                }
                break;
            }
        }

        if(intVals != 1){
            sb.append("0,");
            sb.append("1");
        }
        */

        for(int i = vals.length - 7; i < vals.length; i++){
            sb.append(",");
            sb.append(vals[i]);
        }
        String csvRow = sb.toString();

        context.write(key, new Text(csvRow));
    }

    public void addIndexesToSeverity(BufferedReader br, int start,int end,HashSet<Integer> severityOne,HashSet<Integer> severityTwo, HashMap<Integer,Integer> key_to_val) throws IOException, InterruptedException{
        String line;
        while ((line = br.readLine()) != null) {
            String[] vals = line.split(",");
            for (int i = start; i <= end; i++) {
                vals[i] = processString(vals[i]);
                int intVals = Integer.parseInt(vals[i]);
                if (key_to_val.containsKey(intVals)) {
                    if (key_to_val.get(intVals) >= 3) {
                        severityTwo.add(i);
                    } else {
                        severityOne.add(i);
                    }
                }
            }
        }
    }

    public void createDictionaryFromFile(BufferedReader br, HashMap<Integer,Integer> key_to_val) throws IOException, InterruptedException{
        String line;
        while ((line = br.readLine()) != null) {
            String[] words = line.split(",");
            words[0] = processString(words[0]);
            words[2] = processString(words[2]);
            key_to_val.put(Integer.parseInt(words[0]), Integer.parseInt(words[2]));
        }
    }

    public String processString(String s){
        return s.replace(" ","");
    }

}
