/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package alcoholbyhour;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author deeptipundlik
 */
public class AlcoholByHour_Mapper extends Mapper<Object,Text,IntWritable,IntWritable>{
    
    private IntWritable count = new IntWritable(1);
    
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String input[] = value.toString().split(",");
        
        if(input[0].trim().equalsIgnoreCase("DATE") || input[1].trim().equalsIgnoreCase("") || input.length < 20){
        
        }
        else if(!(input[19].trim().equalsIgnoreCase("Alcohol Involvement"))){
        
        }
        else{
            
            String stringTime[] = input[1].split(":");
            int time = Integer.parseInt(stringTime[0]);
            context.write(new IntWritable(time), count);
        }
        
        
    }
    
    
    
}
