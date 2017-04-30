import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;


/**
 * Created by anish on 4/30/17.
 */
public class DataTransformerMapper
        extends Mapper<LongWritable, Text, LongWritable, Text> {

    Map<String, List<String>> cuisineMapping = new LinkedHashMap<>();
    Map<String, List<String>> vioMapping = new LinkedHashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        cuisineMapping.put("American", new ArrayList<>(Arrays.asList("Cajun", "Steak", "Bakery", "Creole", "Donuts", "Chicken", "Chilean", "Hotdogs", "cuisine", "American", "Barbecue", "Hawaiian", "Moroccan", "Peruvian", "Brazilian", "Caribbean", "Hamburgers", "Californian", "Creole/Cajun", "Southwestern", "Bagels/Pretzels", "Hotdogs/Pretzels", "Pancakes/Waffles", "Latin (Cuban, Dominican, Puerto Rican, South & Central American)")));
        cuisineMapping.put("Asian", new ArrayList<>(Arrays.asList("Thai", "Asian", "Korean", "Chinese", "Filipino", "Japanese", "Indonesian", "Polynesian", "Chinese/Cuban", "Chinese/Japanese", "Vietnamese/Cambodian/Malaysia")));
        cuisineMapping.put("Continental", new ArrayList<>(Arrays.asList("Czech","Greek","Irish","Other","Soups","French","German","Polish","Salads","African","English","Russian","Seafood","Spanish","Armenian","Ethiopian","Soul Food","Australian","Portuguese","Sandwiches","Continental","Delicatessen","Scandinavian","Jewish/Kosher","Eastern European","Fruits/Vegetables","CafÃ©/Coffee/Tea","Nuts/Confectionary","Soups & Sandwiches","Not Listed/Not Applicable","Juice, Smoothies, Fruit Salads","Sandwiches/Salads/Mixed Buffet","Ice Cream, Gelato, Yogurt, Ices","Bottled beverages, including water, sodas, juices, etc.")));
        cuisineMapping.put("Indian", new ArrayList<>(Arrays.asList("Indian","Pakistani","Vegetarian","Bangladeshi")));
        cuisineMapping.put("Italian", new ArrayList<>(Arrays.asList("Pizza","Italian","Pizza/Italian")));
        cuisineMapping.put("Mexican", new ArrayList<>(Arrays.asList("Tapas","Mexican","Tex-Mex")));
        cuisineMapping.put("Middle Eastern", new ArrayList<>(Arrays.asList("Afghan","Iranian","Turkish","Egyptian","Mediterranean","Middle Eastern")));

        vioMapping.put("facilities", new ArrayList<>(Arrays.asList("03E","04J","05A","05B","05C","05F","05H","05I","06G","06H","06I","07A","08C","09C","10B","10C","10D","10E","10F","10G","15I","15J","15K","15L","15S","15T","18B","18C","18D","18F","20A","20B","20D","20E","20F","22A","22C","22E")));
        vioMapping.put("food handling", new ArrayList<>(Arrays.asList("02A","02B","02C","02D","02E","02F","02G","02H","02I","02J","03A","03B","03C","03D","03F","03G","04A","04E","04H","09A","09B","10I","15E","15H","16A","16B","16C","16E","16F","22G")));
        vioMapping.put("hygiene", new ArrayList<>(Arrays.asList("04B","04C","04D","04F","04G","04I","05D","05E","06A","06B","06C","06D","06E","06F","08B","10A","10H","10J","22B","22F")));
        vioMapping.put("vermin", new ArrayList<>(Arrays.asList("04K","04L","04M","04N","04O","08A")));
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        if (key.get() == 0) return;

        String line = value.toString();
        String[] vals = line.split("\t");

        String csvRow = "";
        String separator = ",";

        csvRow += vals[0] + separator + vals[1];

        for (String cuisine: cuisineMapping.keySet()) {
            if (cuisineMapping.get(cuisine).contains(vals[2]))
                csvRow += separator + "1";
            else
                csvRow += separator + "0";
        }

        for (String violation: vioMapping.keySet()) {
            if (vioMapping.get(violation).contains(vals[3]))
                csvRow += separator + "1";
            else
                csvRow += separator + "0";
        }

        csvRow += separator + vals[4] + separator + vals[5] + separator + vals[6];

        context.write(key, new Text(csvRow));
    }
}
