/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package accidentsbycounty;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author deeptipundlik
 */
public class AccidentsByCounty_Mapper extends Mapper<Object, Text, Text, IntWritable> {

    private IntWritable count = new IntWritable(1);
    
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        
         String input[] = value.toString().split(",");
        
        if(input[0].trim().equalsIgnoreCase("DATE") || input[2].trim().equalsIgnoreCase("") || input[2].trim().equalsIgnoreCase("0")
                    || input[0].trim().equalsIgnoreCase("")){
        
        }else{
           
               
                String borough = input[2];
               
                context.write(new Text(borough), count);
            
        }
        
    }
    
    
    
}
