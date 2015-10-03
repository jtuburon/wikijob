/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uniandes.bigdata.taller02;

import java.util.regex.Pattern;

/**
 *
 * @author teo
 */

/* 
(\\[\\[(.*)\\]\\])+
(\\[\\[(.*)\\]\\])+

*/
public class FilteringPatterns {    
    public static Pattern INFO_BOX_PATTERN = Pattern.compile("\\{\\{Infobox\\s+(.*?)'''", Pattern.DOTALL);
    
    public static String BIRTH_PLACE_TOKEN="birth_place";
    public static String DEATH_PLACE_TOKEN="death_place";
    public static String BIRTH_DATE_TOKEN="birth_date";
    public static String DEATH_DATE_TOKEN="death_date";
    public static String SPOUSE_TOKEN="spouse";
    public static String CHILDREN_TOKEN="children";
    
    
    public static String BIRTH_PLACE_KEY="born_in";
    public static String DEATH_PLACE_KEY="died_in";
    public static String BIRTH_DATE_KEY="born_on";
    public static String DEATH_DATE_KEY="died_on";
    public static String SPOUSE_KEY="married_with";
    public static String CHILDREN_KEY="son_or_daughter";
    
    
    
    
    public static Pattern BIRTH_PLACE_PATTERN = Pattern.compile("\\|(\\s*?)birth_place(\\s*?)=(\\s*?)(((,\\s+)?\\[\\[(.*?)\\]\\])|(,\\s+)?(\\w+))+(\\s+)?\n");
    public static Pattern DEATH_PLACE_PATTERN = Pattern.compile("\\|(\\s*)death_place(\\s*)=(\\s*)(((,\\s+)?\\[\\[(.*)\\]\\])|(,\\s+)?(\\w+))+(\\s+)?\n");
    
    public static Pattern BIRTH_DATE_PATTERN = Pattern.compile("\\|(\\s*)birth_date(\\s*)=(\\s*)\\{\\{(.*?)\\}\\}\n");
    public static Pattern DEATH_DATE_PATTERN = Pattern.compile("\\|(\\s*)death_date(\\s*)=(\\s*)\\{\\{(.*?)\\}\\}\n");
    
    public static Pattern SPOUSE_PATTERN = Pattern.compile("\\|(\\s*)spouse(\\s*?)=(\\s*?)(?<spouseinfo>[^\\n]+?)\\n");
    public static Pattern SPOUSE_PATTERN_PERSON = Pattern.compile("\\[\\[(?<personinfo>.*?)\\]\\]");
    
    public static Pattern CHILDREN_PATTERN = Pattern.compile("\\|(\\s*)children(\\s*?)=(\\s*?)(?<childreninfo>[^\\n]+?)\\n");
    public static Pattern CHILDREN_PATTERN_PERSON = Pattern.compile("\\[\\[(?<personinfo>.*?)\\]\\]");
    
}
