/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package contributingfactormalefemale;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author deeptipundlik
 */
public class CompositeKeyWritable implements Writable, WritableComparable<CompositeKeyWritable>{

    
    private String factor;
    private String gender;

    public String getFactor() {
        return factor;
    }

    public void setFactor(String factor) {
        this.factor = factor;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }
    
    
    public CompositeKeyWritable(){
    
    }
    
    public CompositeKeyWritable(String factor, String gender){
        this.factor = factor;
        this.gender = gender;
    }
    
    @Override
    public void write(DataOutput d) throws IOException {
        d.writeUTF(factor);
        d.writeUTF(gender);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        factor = di.readUTF();
        gender = di.readUTF();
    }

    @Override
    public int compareTo(CompositeKeyWritable o) {
        int result = factor.compareTo(o.factor);
        if(result == 0){
            result = gender.compareTo(o.gender);
        }
        return result;
    }

    @Override
    public String toString() {
        return (this.factor + "\t" + this.gender);
    }
    
    
    
}

