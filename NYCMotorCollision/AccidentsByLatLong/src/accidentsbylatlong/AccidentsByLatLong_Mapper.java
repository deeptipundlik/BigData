/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package accidentsbylatlong;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author deeptipundlik
 */
public class AccidentsByLatLong_Mapper extends Mapper<Object,Text,CompositeKeyWritable,CompositeValueWritable> {
    
  
    private CompositeKeyWritable latLongKey = new CompositeKeyWritable();
    private CompositeValueWritable injuredKilledCount = new  CompositeValueWritable();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        
        String input[] = value.toString().split(",");
        
            System.out.println("Input length is : " + input.length);
        
        
        if(input[0].trim().equalsIgnoreCase("DATE") ||input.length < 12 || input[4].trim().equals("") || input[5].trim().equalsIgnoreCase("")
                || input[4].trim().equalsIgnoreCase("0.0") || input[5].trim().equalsIgnoreCase("0")|| input[11].trim().equalsIgnoreCase("") || input[12].trim().equalsIgnoreCase("") ){
        
        }
        else{
        
           double latitude = Double.parseDouble(input[4]);
           double longitude = Double.parseDouble(input[5]);
           latLongKey.setLatitude(latitude);
           latLongKey.setLongitude(longitude);
           int noOfAccidents = 1;
           int noOfInjured = Integer.parseInt(input[11]);
           int noOfKilled = Integer.parseInt(input[12]);
           String borough = input[2];
           injuredKilledCount.setNoOfAccidents(noOfAccidents);
           injuredKilledCount.setNoOfInjured(noOfInjured);
           injuredKilledCount.setNoOfKilled(noOfKilled);
           injuredKilledCount.setBoroughs(borough);
           context.write(latLongKey, injuredKilledCount);
           
        }
        
    }
    
    
    
    
}
