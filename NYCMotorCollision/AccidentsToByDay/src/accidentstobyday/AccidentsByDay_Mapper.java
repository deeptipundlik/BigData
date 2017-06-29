/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package accidentstobyday;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author deeptipundlik
 */
public class AccidentsByDay_Mapper extends Mapper<Object,Text,Text,IntWritable> {

    private IntWritable count = new IntWritable(1);
    
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String input[] = value.toString().split(",");
        
        if(input[0].trim().equalsIgnoreCase("DATE") || input[0].trim().equalsIgnoreCase("")){
        
        }else{
        
            try {
                String inputDate[] = input[0].split("/");
                System.out.println("Input length: " + input.length);
                System.out.println("Input Date length: " + inputDate.length);
                
                
                
                if((inputDate.length >  2)){
                    System.out.println("inside th if condition: "+ input[0]);
                    DateFormat format = new SimpleDateFormat("MM/dd/yyyy");
                   
                    Date date = format.parse(input[0]);
                    Calendar cal =  Calendar.getInstance();
                    cal.setTime(date);
                    
                    System.out.println("Date value: "+ date);
                    int weekday  = cal.get(Calendar.DAY_OF_WEEK);
                    SimpleDateFormat newFormat = new SimpleDateFormat("EEEE");
                    String finalDay =newFormat.format(date);
                    System.out.println("Day of the week is  :"+ finalDay);
                    System.out.println("Number of th week is : " + weekday);
                    
                    String nameWeekDay= null;
                    switch(weekday){
                        case 1: nameWeekDay = "Sunday";
                                break;
                        case 2: nameWeekDay = "Monday";
                                break;
                        case 3: nameWeekDay = "Tuesday";
                                break;
                        case 4: nameWeekDay = "Wednesday";
                                break;
                        case 5: nameWeekDay = "Thursday";
                                break; 
                        case 6: nameWeekDay = "Firday";
                                break;   
                        case 7: nameWeekDay = "Saturday";
                                break;        
                    
                    }
                    System.out.println("Name of the week is: " + nameWeekDay);
                    context.write(new Text(nameWeekDay), count);
                    
                }
            } catch (ParseException ex) {
                System.out.println("Exception is :"+ ex.getLocalizedMessage());
            }
    }
    
    }  
    
}
