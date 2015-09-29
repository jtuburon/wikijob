package uniandes.bigdata.taller02.sampler;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SamplerReducer extends Reducer<Text, Text, Text, Text> {

    private final static int TOTAL = 1000000;

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int counter = 0;
        for (Text value : values) {
            if (counter < TOTAL) {
                context.write(key, value);
                counter++;
            }
        }
    }
}
