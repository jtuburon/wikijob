/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uniandes.bigdata.taller02;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.Text;

/**
 *
 * @author teo
 */
public class TestPattern {

    public TestPattern() {
        String cad = "|death_date         = \n"
                + "|death_place        = \n"
                + "|nationality        = [[Colombian people|Colombian]]\n"
                + "|party              = [[Party of the U]] &lt;small&gt;(2005-present)\n"
                + "|otherparty         = [[Colombian Liberal Party|Liberal]]  &lt;small&gt;(1977-2005)\n"
                + "|spouse             = \n"
                + "|relations          = [[Victoriana Mejía Marulanda]] &lt;small&gt;(sister)\n"
                + "|children           = \n"
                + "|alma_mater         = [[Michigan State University]] &lt;small&gt;([[Bachelor of Arts|BA]], [[Master of Arts|MA]])\n"
                + "|profession         = [[Economist]]\n"
                + "|religion           = ";

        Pattern CHILDREN_PATTERN = Pattern.compile("\\|(\\s*)children(\\s*?)=(\\s*?)(?<childreninfo>[^\\n]+?)\\n");
        Matcher childrenMatcher = CHILDREN_PATTERN.matcher(cad);
        if (childrenMatcher.find()) {
            String info = childrenMatcher.group("childreninfo");
            Matcher personMatcher = FilteringPatterns.CHILDREN_PATTERN_PERSON.matcher(info);
            while (personMatcher.find()) {
                String s_person = personMatcher.group("personinfo");
                s_person = s_person.split("\\|")[0];
                System.out.println("Person: " + s_person);
            }
        }
        System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
        Pattern SPOUSE_PATTERN = Pattern.compile("\\|(\\s*)spouse(\\s*?)=(\\s*?)(?<spouseinfo>[^\\n]+?)\\n");
        Matcher spouseMatcher = SPOUSE_PATTERN.matcher(cad);
        if (spouseMatcher.find()) {
            String info = spouseMatcher.group("spouseinfo");
            Matcher personMatcher = FilteringPatterns.SPOUSE_PATTERN_PERSON.matcher(info);
            while (personMatcher.find()) {
                String s_person = personMatcher.group("personinfo");
                s_person = s_person.split("\\|")[0];
                System.out.println("Person: " + s_person);
            }
        }

    }

    public static void main(String[] args) {
        new TestPattern();
    }
}
