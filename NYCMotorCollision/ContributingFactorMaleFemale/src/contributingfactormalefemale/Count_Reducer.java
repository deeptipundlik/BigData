/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package contributingfactormalefemale;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author deeptipundlik
 */
public class Count_Reducer extends Reducer<CompositeKeyWritable,IntWritable,CompositeKeyWritable,IntWritable> {
    
    
    private CompositeKeyWritable result = new CompositeKeyWritable();
    private IntWritable count = new IntWritable();

    @Override
    protected void reduce(CompositeKeyWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        
        int sum =0;
        for(IntWritable val: values){
            sum += val.get();
        }
        count.set(sum);
        context.write(key,count);
    }
    
    
    
}
