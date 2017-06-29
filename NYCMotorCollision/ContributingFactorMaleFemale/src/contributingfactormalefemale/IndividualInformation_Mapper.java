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
public class IndividualInformation_Mapper extends Mapper<Object, Text,IntWritable, Text> {
    
    private IntWritable caseId = new IntWritable();
    private Text gender = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        
        String input[] = value.toString().split(",");
        
        if(input[0].trim().equalsIgnoreCase("Year") || input[2].trim().equalsIgnoreCase("") || 
                input.length <9 || input[8].trim().equalsIgnoreCase("U") || input[8].trim().equalsIgnoreCase("")){
        
        }
        else{
            int caseVehicleId = Integer.parseInt(input[2]);
            String genderText = "G" + input[8];
            caseId.set(caseVehicleId);
            gender.set(genderText);
            context.write(caseId, gender);
        
        }
    }
    
    
    
}
