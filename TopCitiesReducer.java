import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class TopCitiesReducer 
    extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        List<String> cityList = new ArrayList<>();

        for (Text val : values) {
            cityList.add(val.toString());
        }

        Collections.sort(cityList, new Comparator<String>() {
            public int compare(String a, String b) {

                double revA = Double.parseDouble(a.split(":")[1]);
                double revB = Double.parseDouble(b.split(":")[1]);

                return Double.compare(revB, revA); // descending order
            }
        });

        int count = 0;

        for (String city : cityList) {

            if (count == 5)
                break;

            context.write(key, new Text(city));
            count++;
        }
    }
}
