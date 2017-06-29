/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package accidentsbymonth;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author deeptipundlik
 */
public class AccidentsByMonth_Reducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
    
    private IntWritable result = new IntWritable();

    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        
        
        int sum =0;
        
        for(IntWritable val: values){
            sum += val.get();
        }
        result.set(sum);
        context.write(key, result);
    }
   
}
