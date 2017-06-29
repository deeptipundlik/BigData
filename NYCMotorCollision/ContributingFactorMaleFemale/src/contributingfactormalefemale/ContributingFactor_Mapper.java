/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package contributingfactormalefemale;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author deeptipundlik
 */
public class ContributingFactor_Mapper extends Mapper<Object,Text,IntWritable,Text>{

    private IntWritable caseId = new IntWritable();
    private Text contriFactor = new Text();
    
    
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String input[] = value.toString().split(",");
        
        if(input[0].trim().equalsIgnoreCase("Year") || input[0].trim().equalsIgnoreCase("") || input.length <15
                ||input[14].trim().equalsIgnoreCase("")){
        
        }
        else if(input[14].trim().equalsIgnoreCase("Not Applicable") || input[14].trim().equalsIgnoreCase("Not Entered")){
        
        }else{
            
            int caseVehicleId = Integer.parseInt(input[1]);
            String factor = "V" + input[14];
            caseId.set(caseVehicleId);
            contriFactor.set(factor);
            context.write(caseId,contriFactor);
            
            
        }
    }
    
    
}
