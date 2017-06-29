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
public class Count_Mapper extends Mapper<Object,Text,CompositeKeyWritable,IntWritable> {
    
    
    private CompositeKeyWritable cv = new CompositeKeyWritable();
    private IntWritable count = new IntWritable(1);

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        
        String input[] = value.toString().split("\t");
        
        String factor = input[0];
        String gender = input[1];
        String genderUpperCase = gender.toUpperCase();
        cv.setFactor(factor);
        cv.setGender(genderUpperCase);
        context.write(cv, count);
       
    } 
    
}
