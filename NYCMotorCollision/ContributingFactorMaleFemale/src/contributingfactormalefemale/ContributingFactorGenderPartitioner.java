/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package contributingfactormalefemale;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 *
 * @author deeptipundlik
 */
public class ContributingFactorGenderPartitioner extends Partitioner<Text,Text> {

    @Override
    public int getPartition(Text key, Text value, int i) {
        int hashCode = key.hashCode() % i;
        return hashCode;
    }

    
    
}
