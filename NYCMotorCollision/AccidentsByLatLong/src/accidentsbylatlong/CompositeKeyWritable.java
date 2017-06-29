/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package accidentsbylatlong;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author deeptipundlik
 */
public class CompositeKeyWritable implements Writable, WritableComparable<CompositeKeyWritable> {

    
    private Double latitude;
    private Double longitude;
    
    
    public CompositeKeyWritable(){
    
    }
    
    public CompositeKeyWritable(double latitude, double longitude){
        this.latitude = latitude;
        this.longitude = longitude;
    
    }

    public Double getLatitude() {
        return latitude;
    }

    public void setLatitude(Double latitude) {
        this.latitude = latitude;
    }

    public Double getLongitude() {
        return longitude;
    }

    public void setLongitude(Double longitude) {
        this.longitude = longitude;
    }
    
    
    
    @Override
    public void write(DataOutput d) throws IOException {
        d.writeDouble(latitude);
        d.writeDouble(longitude);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        latitude = di.readDouble();
        longitude = di.readDouble();
    }

    @Override
    public int compareTo(CompositeKeyWritable o) {
        int result = latitude.compareTo(o.latitude);
        if(result == 0){
            result = longitude.compareTo(o.longitude);
        }
        
        return result;
    }

    @Override
    public String toString() {
        return (this.latitude + "\t" + this.longitude);
    }
    
    
    
}
