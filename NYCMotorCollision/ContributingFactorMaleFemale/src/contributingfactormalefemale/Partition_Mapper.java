/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package contributingfactormalefemale;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author deeptipundlik
 */
public class Partition_Mapper  extends Mapper<Object,Text,Text,Text>{

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String input[] = value.toString().split("\t");
        
        String factorCount = input[0] + "\t" + input[2];
        String gender = input[1];
        String toUpperCase = gender.toUpperCase();
        if(toUpperCase.equalsIgnoreCase("F") || toUpperCase.equalsIgnoreCase("M")){
            context.write(new Text(toUpperCase), new Text(factorCount));
        }
        
    }
 
    
    
}
