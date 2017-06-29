/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package naiivesbayesaccidentprediction;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author deeptipundlik
 */
public class ClassiferMapper extends Mapper<Object, Text, KeyClassWritable,IntWritable> {
    
    private IntWritable count = new IntWritable(1);
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        
        String input[] = value.toString().split(",");
        
        if(input[0].trim().equalsIgnoreCase("Light Condition")){
        
        }
        else{
            KeyClassWritable k = new KeyClassWritable();
            //KeyClassWritable classKey = new KeyClassWritable();
            int classIndex = (input.length)-1;
            for(int i=0;i<(input.length);i++){
                if(i == 3){
                    System.out.println("CLASS key "+ input[3]);
                    System.out.println("CLASS class key " + input[classIndex]);
                    k.setKey("CLASS");
                    k.setClassKey(input[classIndex]);
                    //context.write(k,count);
                }
                else{
                    System.out.println("Key is :" + input[i]);
                    System.out.println("Class  is :" + input[classIndex]);
                    k.setKey(input[i]);
                    k.setClassKey(input[classIndex]);
                   
                    
                }
                 context.write(k, count);
            }
           
            
            
         
        }
                
    }
    
    
    
}
