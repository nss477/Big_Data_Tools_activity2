import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class CategoryGroupingMapper extends Mapper<LongWritable, Text, Text, Text> {

    Text category = new Text();
    Text cityRevenue = new Text();

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        String[] parts = line.split("\t");   // split using TAB

        if(parts.length == 2){

            String[] keyParts = parts[0].split("\\|");

            if(keyParts.length == 2){

                String cat = keyParts[0];
                String city = keyParts[1];
                String revenue = parts[1];

                category.set(cat);
                cityRevenue.set(city + ":" + revenue);

                context.write(category, cityRevenue);
            }
        }
    }
}
