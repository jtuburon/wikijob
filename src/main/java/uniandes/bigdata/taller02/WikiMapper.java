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
        }

        Pattern p = Pattern.compile("(birth_date)|(birth_place)|(death_date)|(death_place)");
        Matcher m = p.matcher(article);
        boolean isAPerson = m.find();
        boolean children_added=false;
        boolean spouses_added=false;

        if (isAPerson) {
            Matcher birthPlaceMatcher = FilteringPatterns.BIRTH_PLACE_PATTERN.matcher(article);
            if (birthPlaceMatcher.find()) {
                String birthPlaceValue = birthPlaceMatcher.group();
                if (birthPlaceValue.contains(country)) {
                    String person = extractPersonName(article);
                    context.write(new Text(person), new Text("born_in: " + country));
                    Matcher birthDateMatcher = FilteringPatterns.BIRTH_DATE_PATTERN.matcher(article);
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
                    
                    Matcher spouseMatcher = FilteringPatterns.SPOUSE_PATTERN.matcher(article);
                    if (spouseMatcher.find()) {
                        String info = spouseMatcher.group("spouseinfo");
                        Matcher personMatcher = FilteringPatterns.SPOUSE_PATTERN_PERSON.matcher(info);
                        while (personMatcher.find()) {
                            String s_person = personMatcher.group("personinfo");
                            s_person= s_person.split("\\|")[0];
                            context.write(new Text(person), new Text("married_with:" + s_person));
                            spouses_added=true;
                        }
                    }
                    
                    Matcher childrenMatcher = FilteringPatterns.CHILDREN_PATTERN.matcher(article);
                    if (childrenMatcher.find()) {
                        String info = childrenMatcher.group("childreninfo");
                        Matcher personMatcher = FilteringPatterns.CHILDREN_PATTERN_PERSON.matcher(info);
                        while (personMatcher.find()) {
                            String s_person = personMatcher.group("personinfo");
                            s_person= s_person.split("\\|")[0];
                            context.write(new Text(person), new Text("son_or_daughter:" + s_person));
                            children_added=true;
                        }
                    }
                }
            }

            Matcher deathPlaceMatcher = FilteringPatterns.DEATH_PLACE_PATTERN.matcher(article);
            if (deathPlaceMatcher.find()) {
                String deathPlaceValue = deathPlaceMatcher.group();
                if (deathPlaceValue.contains(country)) {
                    String person = extractPersonName(article);
                    context.write(new Text(person), new Text("died_in: " + country));
                    Matcher deathDateMatcher = FilteringPatterns.DEATH_DATE_PATTERN.matcher(article);
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
                        Matcher spouseMatcher = FilteringPatterns.SPOUSE_PATTERN.matcher(article);
                        if (spouseMatcher.find()) {
                            String info = spouseMatcher.group("spouseinfo");
                            Matcher personMatcher = FilteringPatterns.SPOUSE_PATTERN_PERSON.matcher(info);
                            while (personMatcher.find()) {
                                String s_person = personMatcher.group("personinfo");
                                s_person= s_person.split("\\|")[0];
                                context.write(new Text(person), new Text("married_with:" + s_person));
                                spouses_added = true;
                            }
                        }
                    }
                    if (!children_added) {
                        Matcher childrenMatcher = FilteringPatterns.CHILDREN_PATTERN.matcher(article);
                        if (childrenMatcher.find()) {
                            String info = childrenMatcher.group("childreninfo");
                            Matcher personMatcher = FilteringPatterns.CHILDREN_PATTERN_PERSON.matcher(info);
                            while (personMatcher.find()) {
                                String s_person = personMatcher.group("personinfo");
                                s_person= s_person.split("\\|")[0];
                                context.write(new Text(person), new Text("son_or_daughter:" + s_person));
                                children_added = true;
                            }
                        }
                    }
                }
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
        }
        return null;
    }
}
