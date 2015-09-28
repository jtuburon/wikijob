package uniandes.bigdata.taller02.sampler;

import java.io.IOException;
import java.util.HashMap;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private final static int TOTAL=1000000;

	@Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
    	int counter = 0;
    	
        if(Math.random() < 0.5){
            if(counter<TOTAL){
                String page = value.toString() + "</page>\n";
                context.write(new Text(""), new Text(page));
                counter++;
            }            
        }        
    }
}
