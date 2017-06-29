/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package naiivesbayesaccidentprediction;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author deeptipundlik
 */
public class NaiveMapper extends Mapper<Object, Text, CompositeKeyWritable, Text> {

    CompositeKeyWritable c = new CompositeKeyWritable();
    
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        
        String input[] = value.toString().split(",");
        System.out.println("Input[0] is " +input[0]);
        System.out.println("Input[1] is " +input[1]);
        System.out.println("Input[2] is " +input[2]);
            
        if(input[0].trim().equalsIgnoreCase("Light Condition")){
            
        }
        else{
            
            c.setLightCondition(input[0]);
            c.setRoadCondition(input[1]);
            c.setWeatherCondition(input[2]);
               context.write(c,value);
        }
     
    }
    
}
