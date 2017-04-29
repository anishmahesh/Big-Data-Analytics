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
public class AreaClassifierMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    class Area {
        String name;
        double latitude;
        double longitude;

        public Area(String name, double latitude, double longitude) {
            this.name = name;
            this.latitude = latitude;
            this.longitude = longitude;
        }
    }

    List<Area> areas = new ArrayList<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        super.setup(context);

        if (context.getCacheFiles() != null
                && context.getCacheFiles().length > 0) {

            BufferedReader br = new BufferedReader(new FileReader(new File("./Zipcode_Lat_Long.csv")));
            String line;
            int i = 0;
            while ((line = br.readLine()) != null) {
                if(i == 0){i=1;}
                else{
                String[] words = line.split(",");
                areas.add(new Area(words[0], Double.parseDouble(words[1]), Double.parseDouble(words[2])));
                }
            }

        }
        super.setup(context);
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        Map<String, Integer> indexMap = new LinkedHashMap<>();
        indexMap.put("drop_latitude", 0);
        indexMap.put("drop_longitude", 1);
        indexMap.put("pick_latitude", 3);
        indexMap.put("pick_longitude", 4);

        String line = value.toString();
        String[] vals = line.split(",");

        double d_lat = Double.parseDouble(vals[indexMap.get("drop_latitude")]);
        double d_lon = Double.parseDouble(vals[indexMap.get("drop_longitude")]);
        double p_lat = Double.parseDouble(vals[indexMap.get("pick_latitude")]);
        double p_lon = Double.parseDouble(vals[indexMap.get("pick_longitude")]);


        double min1 = Double.MAX_VALUE;
        double min2 = Double.MAX_VALUE;
        String closestArea1 = areas.get(0).name;
        String closestArea2 = areas.get(0).name;
        for (Area a: areas) {
            double dist1 = distance(d_lat, a.latitude, d_lon, a.longitude, 0, 0);
            if (dist1 <= min1) {
                min1 = dist1;
                closestArea1 = a.name;
            }
            double dist2 = distance(p_lat, a.latitude, p_lon, a.longitude, 0, 0);
            if (dist2 <= min2) {
                min2 = dist2;
                closestArea2 = a.name;
            }
        }

        String csvRow = closestArea1 + "," + closestArea2 + "," + line;

        context.write(key, new Text(csvRow));
    }

    /**
     * Calculate distance between two points in latitude and longitude taking
     * into account height difference. If you are not interested in height
     * difference pass 0.0. Uses Haversine method as its base.
     *
     * lat1, lon1 Start point lat2, lon2 End point el1 Start altitude in meters
     * el2 End altitude in meters
     * @returns Distance in Meters
     */
    public double distance(double lat1, double lat2, double lon1,
                                  double lon2, double el1, double el2) {

        final int R = 6371; // Radius of the earth

        double latDistance = Math.toRadians(lat2 - lat1);
        double lonDistance = Math.toRadians(lon2 - lon1);
        double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        double distance = R * c * 1000; // convert to meters

        double height = el1 - el2;

        distance = Math.pow(distance, 2) + Math.pow(height, 2);

        return Math.sqrt(distance);
    }
}
