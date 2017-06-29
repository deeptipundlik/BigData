/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package accidentsbymonth;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author deeptipundlik
 */
public class AccidentsByMonth_Mapper extends Mapper<Object, Text, IntWritable,IntWritable>{

    private IntWritable count = new IntWritable(1);
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String input[] = value.toString().split(",");
        
        if(input[0].trim().equalsIgnoreCase("DATE") || input[0].trim().equalsIgnoreCase("")){
        
        }else{
        
            String inputDate[] = input[0].split("/");
            System.out.println("Input length: " + input.length);
            System.out.println("Input Date length: " + inputDate.length);
            if(!(inputDate.length < 3)){
                int month = Integer.parseInt(inputDate[0]); 
                context.write(new IntWritable(month), count);
            }
            
            
        }
    }
    
    
    
    
}
