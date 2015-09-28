/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uniandes.bigdata.taller02.sampler;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import uniandes.bigdata.taller02.FilteringInputParams;
import uniandes.bigdata.taller02.WikiMapper;
import uniandes.bigdata.taller02.WikiReducer;

/**
 *
 * @author teo
 */
public class SamplerJob {
    
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Se necesitan las carpetas de entrada y salida");
            System.exit(-1);
        }
        String entrada = args[0]; 
        String salida = args[1];
        
        try {
            ejecutarJob(entrada, salida);           
        } catch (Exception e) { 
            e.printStackTrace();
        }

    }

    public static void ejecutarJob(String entrada, String salida) throws IOException, ClassNotFoundException, InterruptedException {

		/**
		 * Objeto de configuración, dependiendo de la versión de Hadoop 
		 * uno u otro es requerido. 
		 *
                 */ 
		Configuration conf = new Configuration();		
                conf.set("textinputformat.record.delimiter","</page>");
                conf.set("mapreduce.output.fileoutputformat.compress", "true");
                conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.BZip2Codec");

                Job wcJob=Job.getInstance(conf, "WikiArticlesExplorer Job");
		wcJob.setJarByClass(SamplerJob.class);
		//////////////////////
		//Mapper
		//////////////////////

		wcJob.setMapperClass(SamplerMapper.class);		
		wcJob.setMapOutputKeyClass(Text.class);
		wcJob.setMapOutputValueClass(Text.class);
		///////////////////////////
		//Reducer
		///////////////////////////
		wcJob.setReducerClass(SamplerReducer.class);
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
