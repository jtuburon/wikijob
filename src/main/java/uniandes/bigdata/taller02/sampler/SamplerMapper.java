/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uniandes.bigdata.taller02.sampler;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import uniandes.bigdata.taller02.FilteringInputParams;
import uniandes.bigdata.taller02.FilteringPatterns;

/**
 *
 * @author teo
 */
public class SamplerMapper extends Mapper<LongWritable, Text, Text, Text> {
    private final static int TOTAL=1000000;
    private static int counter=0;
    @Override
    protected void map(LongWritable key, Text value,
            Mapper.Context context)
            throws IOException, InterruptedException {
        double random=Math.random();
        if(random>= 0.6d && random< 0.7d){
            if(counter<TOTAL){
                String page = value.toString() + "</page>\n";
                context.write(new Text(""), new Text(page));
                counter++;
            }            
        }        
    }
}