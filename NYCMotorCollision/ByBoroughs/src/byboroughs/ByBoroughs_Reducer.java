/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package byboroughs;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author deeptipundlik
 */
public class ByBoroughs_Reducer extends Reducer<CompositeKeyYearBorough,IntWritable,CompositeKeyYearBorough,IntWritable> {

    @Override
    protected void reduce(CompositeKeyYearBorough key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int noOfYears =6;
        int sum =0;
        for(IntWritable val: values){
            sum += val.get();
        }
        
        int average  = sum /noOfYears;
        context.write(key,new IntWritable(average));
    }
    
    
    
    
}
