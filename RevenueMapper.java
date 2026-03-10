import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class RevenueMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private Text categoryCity = new Text();
    private DoubleWritable revenueValue = new DoubleWritable();

    public void map(LongWritable key, Text value, Context context) 
            throws IOException, InterruptedException {

        String line = value.toString();

        // Skip header line
        if (line.startsWith("TransactionID")) {
            return;
        }

        String[] fields = line.split(",");

        try {

            String category = fields[3];
            String city = fields[4];
            int quantity = Integer.parseInt(fields[5]);
            double price = Double.parseDouble(fields[6]);

            double revenue = quantity * price;

            categoryCity.set(category + "|" + city);
            revenueValue.set(revenue);

            context.write(categoryCity, revenueValue);

        } catch (Exception e) {
            // Ignore malformed records
        }
    }
}
