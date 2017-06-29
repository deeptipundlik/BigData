/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package byboroughs;

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
public class ByBoroughs_Mapper extends Mapper<Object,Text,CompositeKeyYearBorough,IntWritable>{
    
    
    private IntWritable count = new IntWritable(1);
    private CompositeKeyYearBorough yearBorough = new CompositeKeyYearBorough();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        
        String input[] = value.toString().split(",");
        
        if(input[0].trim().equalsIgnoreCase("DATE") || input[2].trim().equalsIgnoreCase("") || input[2].trim().equalsIgnoreCase("0")
                    || input[0].trim().equalsIgnoreCase("")){
        
        }else{
           
                
                System.out.println("Inside else condition: " + input[0]);
                String inputDate[] = input[0].trim().split("/");
                int year = Integer.parseInt(inputDate[2]);
                
                System.out.println("Year is : " + year);
                String borough = input[2];
                yearBorough.setBorough(borough);
                yearBorough.setYear(year);
                context.write(yearBorough, count);
            
        }
        
    }
    
    
    
}
