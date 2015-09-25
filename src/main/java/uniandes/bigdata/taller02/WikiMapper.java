/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uniandes.bigdata.taller02;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang.StringEscapeUtils;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WikiMapper extends Mapper<LongWritable, Text, Text, Text> {    
    
    @Override
    protected void map(LongWritable key, Text value,
            Context context)
            throws IOException, InterruptedException {
        String country= context.getConfiguration().get(FilteringInputParams.COUNTRY_FILTER_PARAM);
        String fromDate= context.getConfiguration().get(FilteringInputParams.FROM_DATE_FILTER_PARAM);
        String toDate= context.getConfiguration().get(FilteringInputParams.TO_DATE_FILTER_PARAM);
        String article = value.toString();
        
        Date startDate= null;
        Date endDate=null;
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        try {            
            startDate = df.parse(fromDate);
            endDate = df.parse(toDate);            
        } catch (ParseException ex) {            
        }
        
        Pattern p = Pattern.compile("(birth_date)|(birth_place)|(death_date)|(death_place)");
        Matcher m = p.matcher(article);
        boolean isAPerson= m.find();
        
        if(isAPerson){
            Matcher birthPlaceMatcher= FilteringPatterns.BIRTH_PLACE_PATTERN.matcher(article);
            if(birthPlaceMatcher.find()){                
                String birthPlaceValue= birthPlaceMatcher.group();                
                if (birthPlaceValue.contains(country)){
                    String person= extractPersonName(article);
                    context.write(new Text(person), new Text("born_in: "+country));
                }
            }
            
            Matcher deathPlaceMatcher= FilteringPatterns.DEATH_PLACE_PATTERN.matcher(article);
            if(deathPlaceMatcher.find()){
                String deathPlaceValue= deathPlaceMatcher.group();
                if (deathPlaceValue.contains(country)){
                    String person= extractPersonName(article);
                    context.write(new Text(person), new Text("died_in: "+country));
                }
            }
            
            Matcher birthDateMatcher= FilteringPatterns.BIRTH_DATE_PATTERN.matcher(article);
            if(birthDateMatcher.find()){                
                String birthDateValue= birthDateMatcher.group(birthDateMatcher.groupCount());
                String vals[]= birthDateValue.split("\\|");
                if(vals.length>3){
                    int index=0;
                    while(!Pattern.matches("\\d{4}", vals[index])){
                        index++;
                    }
                    String year= vals[index];
                    String month= vals[index+1];
                    String day= vals[index+2];
                    
                    Date birthDate;
                    try {
                        String dateVal= year+"-"+month+"-"+ day;
                        System.out.println(dateVal);
                        birthDate = df.parse(dateVal);                        
                        if (birthDate.after(startDate) && birthDate.before(endDate)) {
                            String person = extractPersonName(article);
                            context.write(new Text(person), new Text("born_on: " + dateVal));
                        }
                        
                    } catch (ParseException ex) {
                        ex.printStackTrace();
                    }                       
                }
            }
            
            Matcher deathDateMatcher= FilteringPatterns.DEATH_DATE_PATTERN.matcher(article);
            if(deathDateMatcher.find()){                
                String deathDateValue= deathDateMatcher.group(deathDateMatcher.groupCount());
                String vals[]= deathDateValue.split("\\|");
                if(vals.length>3){
                    int index=0;
                    while(!Pattern.matches("\\d{4}", vals[index])){
                        index++;
                    }
                    String year= vals[index];
                    String month= vals[index+1];
                    String day= vals[index+2];
                    
                    Date deathDate;
                    try {
                        String dateVal= year+"-"+month+"-"+ day;
                        System.out.println(dateVal);
                        deathDate = df.parse(dateVal);                        
                        if (deathDate.after(startDate) && deathDate.before(endDate)) {
                            String person = extractPersonName(article);
                            context.write(new Text(person), new Text("died_on: " + dateVal));
                        }
                    } catch (ParseException ex) {
                        ex.printStackTrace();
                    }                       
                }else{
                    System.out.println("No tiene fechas");
                }
            }
        }
    }
    
    private String extractPersonName(String article){
        Pattern p = Pattern.compile("<title>(.*)</title>");
        Matcher m = p.matcher(article);
        if (m.find()) {
            String person_name = m.group(1);
            return person_name;
        }else{
            return null;
        }
    }
}
