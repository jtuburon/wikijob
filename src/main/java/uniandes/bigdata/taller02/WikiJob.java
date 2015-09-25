/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author teo
 */

package uniandes.bigdata.taller02;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WikiJob {
    
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Se necesitan las carpetas de entrada y salida");
            System.exit(-1);
        }
        String entrada = args[0]; 
        String salida = args[1];
        String country = args[2];
        String fromDate = args[3];
        String toDate = args[4];

        try {
            ejecutarJob(entrada, salida, country, fromDate, toDate);           
        } catch (Exception e) { 
            e.printStackTrace();
        }

    }

    public static void ejecutarJob(String entrada, String salida, String country, String fromDate, String toDate) throws IOException, ClassNotFoundException, InterruptedException {

		/**
		 * Objeto de configuración, dependiendo de la versión de Hadoop 
		 * uno u otro es requerido. 
		 *
                 */ 
		Configuration conf = new Configuration();		
                //conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
                //conf.set(XmlInputFormat.END_TAG_KEY, "</page>");
                conf.set("textinputformat.record.delimiter","</page>");

                
                conf.set(FilteringInputParams.COUNTRY_FILTER_PARAM, country);
                conf.set(FilteringInputParams.FROM_DATE_FILTER_PARAM, fromDate);
                conf.set(FilteringInputParams.TO_DATE_FILTER_PARAM, toDate);
                
                conf.set("io.serializations","org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
                
		Job wcJob=Job.getInstance(conf, "WikiArticlesExplorer Job");
		wcJob.setJarByClass(WikiJob.class);
		//////////////////////
		//Mapper
		//////////////////////

		wcJob.setMapperClass(WikiMapper.class);		
		wcJob.setMapOutputKeyClass(Text.class);
		wcJob.setMapOutputValueClass(Text.class);
		///////////////////////////
		//Reducer
		///////////////////////////
		wcJob.setReducerClass(WikiReducer.class);
		wcJob.setOutputKeyClass(Text.class);
		wcJob.setOutputValueClass(Text.class);
		
		///////////////////////////
		//Input Format
		///////////////////////////
		//Advertencia: Hay dos clases con el mismo nombre, 
		//pero no son equivalentes. 
		//Se usa, en este caso, org.apache.hadoop.mapreduce.lib.input.TextInputFormat
		TextInputFormat.setInputPaths(wcJob, new Path(entrada));
		//wcJob.setInputFormatClass(XmlInputFormat.class); 
                wcJob.setInputFormatClass(TextInputFormat.class); 
          	
		////////////////////
		///Output Format
		//////////////////////
		TextOutputFormat.setOutputPath(wcJob, new Path(salida));
		wcJob.setOutputFormatClass(TextOutputFormat.class);
		wcJob.waitForCompletion(true);
		System.out.println(wcJob.toString());
	}
}
