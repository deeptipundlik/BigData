/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package top5contributingfactors;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author deeptipundlik
 */
public class Top5ContributingFactorMapper extends Mapper<Object, Text, Text, IntWritable> {

    private IntWritable count = new IntWritable(1);
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        
        String input[] = value.toString().split(",");
        if(input[0].trim().equalsIgnoreCase("Year") || input.length <16 || input[4].trim().equalsIgnoreCase("Not Applicable") || input[4].trim().equalsIgnoreCase("Not Entered")){
        
        
        }
        else{   
            
                if(input[14].equalsIgnoreCase("")){
                
                }
                else{
                    context.write(new Text(input[14]),count);
                }
            
        }
        
    }
    
    
    
    
}
