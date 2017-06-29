/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package top5contributingfactors;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author deeptipundlik
 */
public class Top5ContributingFactorReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        
        int sum =0;
        
        for(IntWritable val: values){
        
            sum += val.get();
        }
        
        context.write(key, new IntWritable(sum));
    }
    
    
    
}
