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

/**
 *
 * @author deeptipundlik
 */
public class CompositeValueWritable implements Writable{

     private Integer noOfInjured;
     private Integer noOfKilled;
     private Integer noOfAccidents;
     private String boroughs;
     
     public CompositeValueWritable(){
     
     }
     
     public CompositeValueWritable(int noOfInjured, int noOfKilled, int noOfAccidents, String borough){
         this.noOfInjured = noOfInjured;
         this.noOfKilled = noOfKilled;
         this.noOfAccidents = noOfAccidents;
         this.boroughs = borough;
         
     }

    public Integer getNoOfInjured() {
        return noOfInjured;
    }

    public void setNoOfInjured(Integer noOfInjured) {
        this.noOfInjured = noOfInjured;
    }

    public Integer getNoOfKilled() {
        return noOfKilled;
    }

    public void setNoOfKilled(Integer noOfKilled) {
        this.noOfKilled = noOfKilled;
    }

    public Integer getNoOfAccidents() {
        return noOfAccidents;
    }

    public void setNoOfAccidents(Integer noOfAccidents) {
        this.noOfAccidents = noOfAccidents;
    }

    public String getBoroughs() {
        return boroughs;
    }

    public void setBoroughs(String boroughs) {
        this.boroughs = boroughs;
    }
     
     
    @Override
    public void write(DataOutput d) throws IOException {
        d.writeInt(noOfInjured);
        d.writeInt(noOfKilled);
        d.writeInt(noOfAccidents);
        d.writeUTF(boroughs);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        noOfInjured = di.readInt();
        noOfKilled = di.readInt();
        noOfAccidents = di.readInt();
        boroughs = di.readUTF();
    }

    @Override
    public String toString() {
        return (this.noOfAccidents + "\t" + this.noOfInjured + "\t" + this.noOfAccidents + "\t" + this.boroughs);
    }
    
    
    
    
}
