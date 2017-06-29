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
public class KeyClassWritable implements Writable,WritableComparable<KeyClassWritable>{

    private String key;
    private String classKey;
    
    
    public KeyClassWritable(){
    
    }
    
    public KeyClassWritable(String key, String classKey){
        this.key = key;
        this.classKey = classKey;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getClassKey() {
        return classKey;
    }

    public void setClassKey(String classKey) {
        this.classKey = classKey;
    }
    
    
    
    @Override
    public void write(DataOutput d) throws IOException {
        d.writeUTF(key);
        d.writeUTF(classKey);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        key = di.readUTF();
        classKey = di.readUTF();
    }

   


    @Override
    public String toString() {
        return (this.key + "\t" + this.classKey);
    }

    @Override
    public int compareTo(KeyClassWritable o) {
        int result = key.compareTo(o.key);
        if(result == 0){
            result = classKey.compareTo(o.classKey);
        }
        return result;
    }
    
    
    
}
