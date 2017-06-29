/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package naiivesbayesaccidentprediction;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author deeptipundlik
 */
public class CompositeKeyWritable implements Writable,WritableComparable<CompositeKeyWritable> {
    
    private String lightCondition;
    private String roadCondition;
    private String weatherCondition;
    
    
    public CompositeKeyWritable(){
    
    
    }
    
    public CompositeKeyWritable(String lightCondition, String roadCondition, String weatherCondition, String crashDescriptor){
        this.lightCondition = lightCondition;
        this.roadCondition = roadCondition;
        this.weatherCondition = weatherCondition;
    
    }

    @Override
    public void write(DataOutput d) throws IOException {
        d.writeUTF(lightCondition);
        d.writeUTF(roadCondition);
        d.writeUTF(weatherCondition);
        
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        lightCondition = di.readUTF();
        roadCondition = di.readUTF();
        weatherCondition = di.readUTF();
        
    }

    @Override
    public int compareTo(CompositeKeyWritable o) {
        int result = lightCondition.compareTo(o.lightCondition);
        if(result ==0){
            result = roadCondition.compareTo(o.roadCondition);
            if(result == 0){
                result = weatherCondition.compareTo(o.weatherCondition);
            }
        }
        return result;
    }

    public String getLightCondition() {
        return lightCondition;
    }

    public void setLightCondition(String lightCondition) {
        this.lightCondition = lightCondition;
    }

    public String getRoadCondition() {
        return roadCondition;
    }

    public void setRoadCondition(String roadCondition) {
        this.roadCondition = roadCondition;
    }

    public String getWeatherCondition() {
        return weatherCondition;
    }

    public void setWeatherCondition(String weatherCondition) {
        this.weatherCondition = weatherCondition;
    }

    @Override
    public String toString() {
        return (this.lightCondition + "\t" + this.roadCondition + "\t" + this.weatherCondition);
    }

   
    
    
}
