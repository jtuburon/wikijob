/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uniandes.bigdata.taller02;

import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author teo
 */
import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WikiReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Context context)
			throws IOException, InterruptedException {
            String data="";
            for(Text t : values) {
                if(data.equals("")){
                    data = t.toString();
                }else{
                    data = data + ":::" + t.toString();
                }
                
            }
            context.write(new Text(key), new Text(data));
	}

}
