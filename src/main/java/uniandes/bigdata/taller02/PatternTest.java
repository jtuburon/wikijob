/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uniandes.bigdata.taller02;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author teo
 */
public class PatternTest {

    public PatternTest() {
        Pattern p = Pattern.compile("\\{\\{Infobox\\s+(.*?)'''", Pattern.DOTALL);
        String value= "nes, books, etc. ARE reliable. Any information added without a source for verification may be removed by anyone else at any time. Thank you for understanding --&gt;\n" +
"{{Infobox person\n" +
"| name               = Steve Jobs\n" +
"| image              = Steve Jobs with red shawl edit2.jpg\n" +
"| caption            = Jobs in 2007\n" +
"| birth_name         = Steven Paul Jobs\n" +
"| birth_date         = {{Birth date|1955|2|24|mf=y}}\n" +
"| birth_place        = San Francisco\n" +
"| death_date         = {{Death date and age|2011|10|5|1955|2|24|mf=y}}\n" +
"| death_place        = [[Palo Alto, California]]\n" +
"| death_cause       = Cancer\n" +
"| nationality        = American\n" +
"| ethnicity          = German and [[Syrian people|Syrian]]\n" +
"| education          = {{plainlist|\n" +
"* [[Homestead High School (Cupertino, California)|Homestead High School]] ('72)\n" +
"* [[Reed College]] (dropped out)\n" +
"}}\n" +
"| occupation         = {{plainlist|\n" +
"* Cofounder, Chairman, and CEO of [[Apple Inc.]]\n" +
"* Funded [[Pixar]]\n" +
"* Founder and CEO of [[NeXT]]\n" +
"}}\n" +
"| known_for          = Pioneer of the [[Microcomputer revolution|personal computer revolution]] with [[Steve Wozniak]]\n" +
"| boards             = {{plainlist| [[The Walt Disney Company]]&lt;ref name=&quot;The Walt Disney Company and Affiliated Companies&quot;&gt;{{cite web|url=http://corporate.disney.go.com/corporate/board_of_directors.html|title=The Walt Disney Company and Affiliated Companies{{spaced ndash | archiveurl = http://www.webcitation.org/6HdFWBRCh | archivedate = June 25, 2013| deadurl=no}} board of directors|publisher=[[The Walt Disney Company]]|accessdate=October 2, 2009}}&lt;/ref&gt;\n" +
"* [[Apple Inc.]]\n" +
"}}\n" +
"| religion           = [[Zen|Zen Buddhism]] (previously [[Lutheran]])&lt;ref name=&quot;JobsBio1&quot; /&gt;\n" +
"| spouse             = [[Laurene Powell Jobs|Laurene Powell]] (m. 1991–2011; his death)\n" +
"| partner            = [[Chrisann Brennan]] (high school girlfriend and Lisa's mother)\n" +
"| children        = {{plainlist|\n" +
"* [[Lisa Brennan-Jobs]] (with Chrisann)\n" +
"* Reed (with Laurene)\n" +
"* Erin (with Laurene)\n" +
"* Eve (with Laurene)\n" +
"}}\n" +
"| parents            = {{plainlist|\n" +
"* Paul and Clara Jobs (adoptive parents)\n" +
"* Joanne Schieble Simpson and Abdulfattah Jandali (biological parents)\n" +
"}}\n" +
"| relatives          = {{plainlist|\n" +
"* [[Mona Simpson]] (biological sister)\n" +
"* Patty Jobs (adopted sister)\n" +
"}}\n" +
"}}\n" +
"'''Steven Paul Jobs''' ({{IPAc-en|ˈ|dʒ|ɒ|b|z}}; February 24, 1955&amp;nbsp;– October 5, 2011) was an American businessman. He was best known as the co-founder, chairman, and chief executive officer (CEO) of [[Apple Inc.]]; CEO and largest shareholder of [[Pixar|Pixar Animation Studios]];&lt;ref name=&quot;jobspix&quot;&gt;{{cite web\n" +
"|url=http://www.businessinsider.com/steve-jobs-at-pixar-versus-apple-2015-3\n" +
"|title=Why execs from other comp";
        Matcher m = p.matcher(value);
        if(m.find()){
            System.out.println("1: " + m.group(1));
        }else{
            System.out.println("No Wapeo");
        }
    }
    public static void main(String[] args) {
        new PatternTest();
    }
}
