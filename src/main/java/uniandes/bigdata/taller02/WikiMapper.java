/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uniandes.bigdata.taller02;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
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

    private final static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    private final static Pattern DATE_PATTERN = Pattern.compile("(\\d{4})\\|(\\d{1,2})\\|(\\d{1,2})");

    @Override
    protected void map(LongWritable key, Text value,
            Context context)
            throws IOException, InterruptedException {
        String country = context.getConfiguration().get(FilteringInputParams.COUNTRY_FILTER_PARAM);
        String fromDate = context.getConfiguration().get(FilteringInputParams.FROM_DATE_FILTER_PARAM);
        String toDate = context.getConfiguration().get(FilteringInputParams.TO_DATE_FILTER_PARAM);
        String article = value.toString();

        Date startDate = null;
        Date endDate = null;
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        try {
            startDate = df.parse(fromDate);
            endDate = df.parse(toDate);
        } catch (ParseException ex) {
            ex.printStackTrace();
        }

        Matcher infoboxMatcher = FilteringPatterns.INFO_BOX_PATTERN.matcher(article);
        if (infoboxMatcher.find()) {
            String infoBox = infoboxMatcher.group(1);
            
            Pattern p = Pattern.compile("(birth_date)|(birth_place)|(death_date)|(death_place)");
            Matcher m = p.matcher(infoBox);
            boolean isAPerson = m.find();
            boolean children_added = false;
            boolean spouses_added = false;
            ArrayList<String> spouses = new ArrayList<>();;
            ArrayList<String> children = new ArrayList<>();;
            
            if (isAPerson) {                
                String[] lines = infoBox.split("\n");
                HashMap<String, String> map= new HashMap<>();
                for (int i = 0; i < lines.length; i++) {
                    String line = lines[i];
                    System.out.println("Line: "+ line);
                    String[] line_vec = line.split("=", 2);
                    if (line_vec.length == 2) {
                        String l_key = line_vec[0];
                        String l_val = line_vec[1];
                        if (l_key.contains(FilteringPatterns.BIRTH_PLACE_TOKEN)) {
                            if (l_val.contains(country)) {
                                map.put(FilteringPatterns.BIRTH_PLACE_KEY, country);
                            }
                        }
                        if (l_key.contains(FilteringPatterns.DEATH_PLACE_TOKEN)) {
                            if (l_val.contains(country)) {
                                map.put(FilteringPatterns.DEATH_PLACE_KEY, country);
                            }
                        }
                        
                        if(l_key.contains(FilteringPatterns.BIRTH_DATE_TOKEN)){
                            String birthDateString = extractDateAsString(l_val);
                            if (birthDateString != null) {
                                Date birthDate = convertToDate(birthDateString);
                                if (birthDate != null) {
                                    if (birthDate.after(startDate) && birthDate.before(endDate)) {
                                        map.put(FilteringPatterns.BIRTH_DATE_KEY, birthDateString);
                                    }
                                }
                            }
                        }
                        
                        if(l_key.contains(FilteringPatterns.DEATH_DATE_TOKEN)){
                            String deathDateString = extractDateAsString(l_val);
                            if (deathDateString != null) {
                                Date deathDate = convertToDate(deathDateString);
                                if (deathDate != null) {
                                    if (deathDate.after(startDate) && deathDate.before(endDate)) {
                                        map.put(FilteringPatterns.DEATH_DATE_KEY, deathDateString);
                                    }
                                }
                            }
                        }
                        
                        if(l_key.contains(FilteringPatterns.SPOUSE_TOKEN)){
                            map.put(FilteringPatterns.SPOUSE_KEY, "");
                            
                            Matcher personMatcher = FilteringPatterns.SPOUSE_PATTERN_PERSON.matcher(l_val);
                            while (personMatcher.find()) {
                                String s_person = personMatcher.group("personinfo");
                                s_person = s_person.split("\\|")[0];
                                spouses.add(s_person);
                            }
                        }
                        
                        if(l_key.contains(FilteringPatterns.CHILDREN_TOKEN)){
                            map.put(FilteringPatterns.CHILDREN_KEY, "");
                            
                            Matcher personMatcher = FilteringPatterns.CHILDREN_PATTERN_PERSON.matcher(l_val);
                            while (personMatcher.find()) {
                                String s_person = personMatcher.group("personinfo");
                                s_person = s_person.split("\\|")[0];
                                children.add(s_person);
                            }
                        }
                    }
                }
                String person = extractPersonName(article);
                if(map.containsKey(FilteringPatterns.BIRTH_PLACE_KEY)){
                    context.write(new Text(person), new Text(FilteringPatterns.BIRTH_PLACE_KEY+ ":" + country));
                    if(map.containsKey(FilteringPatterns.BIRTH_DATE_KEY)){
                        context.write(new Text(person), new Text(FilteringPatterns.BIRTH_DATE_KEY+ ":"  + map.get(FilteringPatterns.BIRTH_DATE_KEY)));
                    }
                    if(map.containsKey(FilteringPatterns.SPOUSE_KEY) && spouses.size()>0){
                        for (int i = 0; i < spouses.size(); i++) {
                            String spouse = spouses.get(i);
                            context.write(new Text(person), new Text(FilteringPatterns.SPOUSE_KEY + ":" + spouse));
                        }
                        spouses_added=true;
                    }
                    
                    if(map.containsKey(FilteringPatterns.CHILDREN_KEY) && children.size()>0){
                        for (int i = 0; i < children.size(); i++) {
                            String child = children.get(i);
                            context.write(new Text(person), new Text(FilteringPatterns.CHILDREN_KEY + ":" + child));
                        }
                        children_added=true;
                    }
                }
                
                if(map.containsKey(FilteringPatterns.DEATH_PLACE_KEY)){                    
                    context.write(new Text(person), new Text(FilteringPatterns.DEATH_PLACE_KEY+ ":" + country));
                    if(map.containsKey(FilteringPatterns.DEATH_DATE_KEY)){
                        context.write(new Text(person), new Text(FilteringPatterns.DEATH_DATE_KEY+ ":" + map.get(FilteringPatterns.DEATH_DATE_KEY)));
                    }
                    if (!spouses_added) {
                        if (map.containsKey(FilteringPatterns.SPOUSE_KEY) && spouses.size() > 0) {
                            for (int i = 0; i < spouses.size(); i++) {
                                String spouse = spouses.get(i);
                                context.write(new Text(person), new Text(FilteringPatterns.SPOUSE_KEY + ":" + spouse));
                            }
                        }
                    }
                    
                    if (!children_added) {
                        if (map.containsKey(FilteringPatterns.CHILDREN_KEY) && children.size() > 0) {
                            for (int i = 0; i < children.size(); i++) {
                                String child = children.get(i);
                                context.write(new Text(person), new Text(FilteringPatterns.CHILDREN_KEY + ":" + child));
                            }
                        }
                    }
                }
                /*
                Matcher birthPlaceMatcher = FilteringPatterns.BIRTH_PLACE_PATTERN.matcher(infoBox);
                if (birthPlaceMatcher.find()) {
                    String birthPlaceValue = birthPlaceMatcher.group();
                    if (birthPlaceValue.contains(country)) {
                        String person = extractPersonName(article);
                        context.write(new Text(person), new Text("born_in: " + country));
                        
                        Matcher birthDateMatcher = FilteringPatterns.BIRTH_DATE_PATTERN.matcher(infoBox);
                        if (birthDateMatcher.find()) {
                            String birthDateValue = birthDateMatcher.group(birthDateMatcher.groupCount());
                            String birthDateString = extractDateAsString(birthDateValue);
                            if (birthDateString != null) {
                                Date birthDate = convertToDate(birthDateString);
                                if (birthDate != null) {
                                    if (birthDate.after(startDate) && birthDate.before(endDate)) {
                                        context.write(new Text(person), new Text("born_on: " + birthDateString));
                                    }
                                }
                            }
                        }

                        Matcher spouseMatcher = FilteringPatterns.SPOUSE_PATTERN.matcher(infoBox);
                        if (spouseMatcher.find()) {
                            String info = spouseMatcher.group("spouseinfo");
                            Matcher personMatcher = FilteringPatterns.SPOUSE_PATTERN_PERSON.matcher(info);
                            while (personMatcher.find()) {
                                String s_person = personMatcher.group("personinfo");
                                s_person = s_person.split("\\|")[0];
                                context.write(new Text(person), new Text("married_with:" + s_person));
                                spouses_added = true;
                            }
                        }

                        Matcher childrenMatcher = FilteringPatterns.CHILDREN_PATTERN.matcher(infoBox);
                        if (childrenMatcher.find()) {
                            String info = childrenMatcher.group("childreninfo");
                            Matcher personMatcher = FilteringPatterns.CHILDREN_PATTERN_PERSON.matcher(info);
                            while (personMatcher.find()) {
                                String s_person = personMatcher.group("personinfo");
                                s_person = s_person.split("\\|")[0];
                                context.write(new Text(person), new Text("son_or_daughter:" + s_person));
                                children_added = true;
                            }
                        }
                    }
                }
                
                Matcher deathPlaceMatcher = FilteringPatterns.DEATH_PLACE_PATTERN.matcher(infoBox);
                if (deathPlaceMatcher.find()) {
                    String deathPlaceValue = deathPlaceMatcher.group();
                    if (deathPlaceValue.contains(country)) {
                        String person = extractPersonName(article);
                        context.write(new Text(person), new Text("died_in: " + country));

                        Matcher deathDateMatcher = FilteringPatterns.DEATH_DATE_PATTERN.matcher(infoBox);
                        if (deathDateMatcher.find()) {
                            String deathDateValue = deathDateMatcher.group(deathDateMatcher.groupCount());
                            String deathDateString = extractDateAsString(deathDateValue);
                            if (deathDateString != null) {
                                Date deathDate = convertToDate(deathDateString);
                                if (deathDate != null) {
                                    if (deathDate.after(startDate) && deathDate.before(endDate)) {
                                        context.write(new Text(person), new Text("born_on: " + deathDateString));
                                    }
                                }
                            }
                        }

                        if (!spouses_added) {
                            Matcher spouseMatcher = FilteringPatterns.SPOUSE_PATTERN.matcher(infoBox);
                            if (spouseMatcher.find()) {
                                String info = spouseMatcher.group("spouseinfo");
                                Matcher personMatcher = FilteringPatterns.SPOUSE_PATTERN_PERSON.matcher(info);
                                while (personMatcher.find()) {
                                    String s_person = personMatcher.group("personinfo");
                                    s_person = s_person.split("\\|")[0];
                                    context.write(new Text(person), new Text("married_with:" + s_person));
                                    spouses_added = true;
                                }
                            }
                        }
                        if (!children_added) {
                            Matcher childrenMatcher = FilteringPatterns.CHILDREN_PATTERN.matcher(infoBox);
                            if (childrenMatcher.find()) {
                                String info = childrenMatcher.group("childreninfo");
                                Matcher personMatcher = FilteringPatterns.CHILDREN_PATTERN_PERSON.matcher(info);
                                while (personMatcher.find()) {
                                    String s_person = personMatcher.group("personinfo");
                                    s_person = s_person.split("\\|")[0];
                                    context.write(new Text(person), new Text("son_or_daughter:" + s_person));
                                    children_added = true;
                                }
                            }
                        }

                    }
                }
                */
            
            }
        }

    }

    private String extractPersonName(String article) {
        Pattern p = Pattern.compile("<title>(.*)</title>");
        Matcher m = p.matcher(article);
        if (m.find()) {
            String person_name = m.group(1);
            return person_name;
        } else {
            return null;
        }
    }

    private String extractDateAsString(String line) {
        Matcher m = DATE_PATTERN.matcher(line);
        if (m.find()) {
            String year = m.group(1);
            String month = m.group(2);
            String day = m.group(3);

            String dateVal = year + "-" + month + "-" + day;
            return dateVal;
        }
        return null;
    }

    private Date convertToDate(String dateString) {
        Date date;
        try {
            date = DATE_FORMAT.parse(dateString);
            return date;
        } catch (ParseException ex) {
            ex.printStackTrace();
        }
        return null;
    }
}
