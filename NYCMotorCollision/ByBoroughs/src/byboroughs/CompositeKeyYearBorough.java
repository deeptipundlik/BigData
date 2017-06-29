/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package byboroughs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author deeptipundlik
 */
public class CompositeKeyYearBorough implements Writable,WritableComparable<CompositeKeyYearBorough>{

    private Integer year;
    private String borough;
    
    public CompositeKeyYearBorough(){
    
    }
    public CompositeKeyYearBorough(int year, String borough){
        this.year = year;
        this.borough = borough;
    
    }

    public Integer getYear() {
        return year;
    }

    public void setYear(Integer year) {
        this.year = year;
    }

    public String getBorough() {
        return borough;
    }

    public void setBorough(String borough) {
        this.borough = borough;
    }
    
    
    
    @Override
    public void write(DataOutput d) throws IOException {
        d.writeInt(year);
        d.writeUTF(borough);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        year = di.readInt();
        borough = di.readUTF();
    }

    @Override
    public int compareTo(CompositeKeyYearBorough o) {
        int result = year.compareTo(o.year);
        if(result == 0){
            result = borough.compareTo(o.borough);
        }
        return result;
    }

    @Override
    public String toString() {
        return (this.year + "\t" + this.borough);
    }
    
    
    
}
