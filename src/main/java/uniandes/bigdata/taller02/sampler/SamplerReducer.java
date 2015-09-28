/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uniandes.bigdata.taller02.sampler;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author teo
 */
public class SamplerReducer extends Reducer<Text, Text, Text, Text> {

    private final static int TOTAL = 1000000;

    protected void reduce(Text key, Iterable<Text> values,
            Reducer.Context context)
            throws IOException, InterruptedException {
        int counter = 0;
        for (Text t : values) {
            if (counter < TOTAL) {
                context.write(key, t);
                counter++;                
            }
        }
    }

}
