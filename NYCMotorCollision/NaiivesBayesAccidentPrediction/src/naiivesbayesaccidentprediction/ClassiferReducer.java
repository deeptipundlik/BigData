/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package naiivesbayesaccidentprediction;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author deeptipundlik
 */
public class ClassiferReducer extends Reducer<KeyClassWritable, IntWritable, KeyClassWritable, DoubleWritable> {
 
    HashMap<KeyClassWritable,Double> probablityTable = new HashMap<>();
    private int totalTrainingSetRecords = 0;
    private int totalYesClassCount = 0;
    private int totalNoClassCount = 0;
   // KeyClassWritable result = new KeyClassWritable();
    
    @Override
    protected void reduce(KeyClassWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum =0;
        KeyClassWritable result = new KeyClassWritable();
        System.out.println("In the reducer " + key);
        
        for(IntWritable val: values){
            if(key.getKey().trim().equalsIgnoreCase("CLASS")){
        
            if(key.getClassKey().trim().equalsIgnoreCase("YES")){
                totalYesClassCount += val.get();
            }
            else{
                totalNoClassCount += val.get();
            }
        }
            sum += val.get();
        }
        
        result.setKey(key.getKey());
        result.setClassKey(key.getClassKey());
        
        probablityTable.put(result, (double)sum);
        
        
        
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.out.println("Hash map size is : " + probablityTable.size());
        int totalrecords = totalNoClassCount + totalYesClassCount;
        System.out.println("Total training records: "+ totalrecords);
        System.out.println("Total Yes records: "+ totalYesClassCount);
        System.out.println("Total NO records: "+ totalNoClassCount);
        KeyClassWritable r = new KeyClassWritable();
        Iterator it = probablityTable.entrySet().iterator();
        while(it.hasNext())
         {
            Map.Entry pair = (Map.Entry)it.next();
            r = (KeyClassWritable)pair.getKey();
            System.out.println("The key of hash map is: " + r);
            double conditionalProbab;
            double value = (double)pair.getValue();
            if(r.getKey().equalsIgnoreCase("CLASS")){
                
                conditionalProbab = value / totalrecords;
            
            }
            else{
                if(r.getClassKey().equalsIgnoreCase("YES")){
                    
                    conditionalProbab = value/totalYesClassCount; 
                }
                else{
                
                    conditionalProbab = value/totalNoClassCount;
                }
            
            }
            
            context.write(r, new DoubleWritable(conditionalProbab));
            it.remove();
        }
    }
    
    
    
    
}
