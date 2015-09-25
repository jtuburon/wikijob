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
    public static Pattern BIRTH_PLACE_PATTERN = Pattern.compile("\\|(\\s*)birth_place(\\s*)=(\\s*)(((,\\s+)?\\[\\[(.*)\\]\\])|(,\\s+)?(\\w+))+(\\s+)?\n");
    public static Pattern DEATH_PLACE_PATTERN = Pattern.compile("\\|(\\s*)death_place(\\s*)=(\\s*)(((,\\s+)?\\[\\[(.*)\\]\\])|(,\\s+)?(\\w+))+(\\s+)?\n");
    
    public static Pattern BIRTH_DATE_PATTERN = Pattern.compile("\\|(\\s*)birth_date(\\s*)=(\\s*)\\{\\{(.*?)\\}\\}\n");
    public static Pattern DEATH_DATE_PATTERN = Pattern.compile("\\|(\\s*)death_date(\\s*)=(\\s*)\\{\\{(.*?)\\}\\}\n");
}
