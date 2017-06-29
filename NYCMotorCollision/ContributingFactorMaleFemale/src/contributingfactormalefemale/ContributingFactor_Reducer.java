/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package contributingfactormalefemale;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author deeptipundlik
 */
public class ContributingFactor_Reducer extends Reducer<IntWritable, Text, Text, Text> {
    private ArrayList<Text> listA = new ArrayList<>();
    private ArrayList<Text> listB = new ArrayList<>();
    
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        
        listA.clear();
        listB.clear();
        
        for(Text val: values){
                
            if (Character.toString((char) val.charAt(0)).equals("V")) {
                listA.add(new Text(val.toString().substring(1)));
            } else if (Character.toString((char) val.charAt(0)).equals("G")) {
                listB.add(new Text(val.toString().substring(1)));
            }
        }
        
        System.out.println("The length is A is #####  "+listA.size());
        System.out.println("The length is B #### "+listB.size());
        
        if(!listA.isEmpty() && !listB.isEmpty()){
            for (Text A : listA) {
                for (Text B : listB) {
                    context.write(A,B);
                }
             }
        
        }
        
        
    }
    
    
    
}
