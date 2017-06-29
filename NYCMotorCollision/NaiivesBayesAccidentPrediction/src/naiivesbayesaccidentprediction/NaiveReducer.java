/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package naiivesbayesaccidentprediction;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author deeptipundlik
 */
public class NaiveReducer extends Reducer<CompositeKeyWritable,Text, CompositeKeyWritable,Text>{
    private static HashMap<KeyClassWritable, Double> probablityTable = new HashMap<>();
    private BufferedReader bufferReader;
    String classifications[] = new String[2] ;

    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Path[] files = context.getLocalCacheFiles();
         System.out.print("The lentgh is "+files.length);
        classifications[0] ="YES";
        classifications[1] = "NO";
        for (Path eachPath : files) {
            System.out.println("File path is : "+eachPath.getName().toString().trim());
            if (eachPath.getName().toString().trim().equals("part-r-00000")) {
                System.out.println("Inside the if condition");
             // context.getCounter(MYCOUNTER.FILE_EXISTS).increment(1);
                bufferReader = new BufferedReader(new FileReader(eachPath.toString()));
                String lineReader = "";
                while ((lineReader = bufferReader.readLine()) != null) {
                    String input[] = lineReader.replace("\"", "").split("\\t");
                    System.out.println("The value is"+input[0]);
                    KeyClassWritable k = new KeyClassWritable();
                    
                    k.setKey(input[0]);
                    k.setClassKey(input[1]);
                    double probab = Double.parseDouble(input[2]);
                    probablityTable.put(k, probab);
                }
                bufferReader.close();
            }
              
        }   
        
        System.out.println("How many classes : " +classifications.length);
        System.out.println("The size of probability table reducer 2: " + probablityTable.size());
        
        
    
    }

    @Override
    protected void reduce(CompositeKeyWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        
        double maxPosterior = 0.0;
        String selectedClass = null;
        for(int i=0;i<classifications.length;i++){
            double posterior = getPosteriorProbablity(classifications[i],key);
            
            if(selectedClass == null){
                selectedClass = classifications[i];
                maxPosterior = posterior;
            }
            else{
                if(posterior > maxPosterior){
                    selectedClass = classifications[i];
                    maxPosterior = posterior;
                }
                
            }
        }
        System.out.println("Reducer Value is : " + selectedClass + " " + maxPosterior);
        String reducerValue = selectedClass + " " + maxPosterior;
        context.write(key,new Text(reducerValue));
    }
    
    public double getPosteriorProbablity(String aClass,CompositeKeyWritable k){
        double classProbablity = 0,lightCondProbab = 0,roadCondProbab = 0,weatherCondProbab = 0.0;
        Iterator it = probablityTable.entrySet().iterator();
        System.out.println("Class is : " + aClass);
        while(it.hasNext())
         {
             KeyClassWritable r = new KeyClassWritable();
            Map.Entry pair = (Map.Entry)it.next();
            r = (KeyClassWritable)pair.getKey();
            System.out.println("Hash map : " + r + (Double)pair.getValue());
            if(r.getClassKey().trim().equalsIgnoreCase(aClass) && r.getKey().trim().equalsIgnoreCase("CLASS")){
                classProbablity = (Double)pair.getValue();
            }
            
            if(r.getKey().trim().equalsIgnoreCase(k.getLightCondition()) && r.getClassKey().trim().equalsIgnoreCase(aClass)){
                lightCondProbab = (Double)pair.getValue();
            }
            if(r.getKey().trim().equalsIgnoreCase(k.getRoadCondition())&& r.getClassKey().trim().equalsIgnoreCase(aClass)){
                roadCondProbab = (Double)pair.getValue();
            }
            if(r.getKey().trim().equalsIgnoreCase(k.getWeatherCondition())&& r.getClassKey().trim().equalsIgnoreCase(aClass)){
                weatherCondProbab = (Double)pair.getValue();
            }
            
         }
        it.remove();
        
        
       
        double finalProbab = classProbablity * lightCondProbab * roadCondProbab * weatherCondProbab;
        System.out.println("Class Probablity: " + classProbablity);
        System.out.println("lightCondProbab Probablity: " + lightCondProbab);
        System.out.println("roadCondProbab Probablity: " + roadCondProbab);
        System.out.println("weatherCondProbab Probablity: " + weatherCondProbab);
        System.out.println("finalProbab Probablity: " + finalProbab);
        
        return finalProbab;
    }
    
    
}
