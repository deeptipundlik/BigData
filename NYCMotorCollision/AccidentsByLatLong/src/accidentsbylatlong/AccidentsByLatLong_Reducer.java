/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package accidentsbylatlong;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author deeptipundlik
 */
public class AccidentsByLatLong_Reducer extends Reducer<CompositeKeyWritable,CompositeValueWritable,CompositeKeyWritable,CompositeValueWritable> {
    
    private CompositeValueWritable result = new CompositeValueWritable();

    @Override
    protected void reduce(CompositeKeyWritable key, Iterable<CompositeValueWritable> values, Context context) throws IOException, InterruptedException {
        
        int sumOfInjured =0;
        int sumofKilled = 0;
        int sumOfAccidents = 0;
        String borough = null;
        
        
        for(CompositeValueWritable val : values){
            sumOfAccidents += val.getNoOfAccidents();
            sumOfInjured += val.getNoOfInjured();
            sumofKilled += val.getNoOfKilled();
            borough = val.getBoroughs();
            
        }
        
        result.setNoOfAccidents(sumOfAccidents);
        result.setNoOfInjured(sumOfInjured);
        result.setNoOfKilled(sumofKilled);
        result.setBoroughs(borough);
        
        context.write(key, result);
        
    }
    
    
    
    
}
