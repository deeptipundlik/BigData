/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package naiivesbayesaccidentprediction;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author deeptipundlik
 */
public class NaiivesBayesAccidentPrediction {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // TODO code application logic here
        
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "Classifier");
        job1.setJarByClass(NaiivesBayesAccidentPrediction.class);
        job1.setMapperClass(ClassiferMapper.class);
        job1.setReducerClass(ClassiferReducer.class);
        job1.setMapOutputKeyClass(KeyClassWritable.class);
        job1.setMapOutputValueClass(IntWritable.class);
        
        
        job1.setOutputKeyClass(KeyClassWritable.class);
        job1.setOutputValueClass(KeyClassWritable.class);
        FileInputFormat.addInputPath(job1,new Path(args[0]));
        FileOutputFormat.setOutputPath(job1,new Path(args[1]));
        
        boolean jobComplete = job1.waitForCompletion(true);
        
        if(jobComplete){
            Configuration conf2 = new Configuration();
            Job job2 = Job.getInstance(conf2,"NaiveBayesAlgorithm");
            job2.addCacheFile(new Path(args[1]+"/part-r-00000").toUri());
            job2.setJarByClass(NaiivesBayesAccidentPrediction.class);
            job2.setMapperClass(NaiveMapper.class);
            job2.setReducerClass(NaiveReducer.class);
            job2.setMapOutputKeyClass(CompositeKeyWritable.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setOutputKeyClass(CompositeKeyWritable.class);
            job2.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job2, new Path(args[2]));
            FileOutputFormat.setOutputPath(job2, new Path(args[3]));
            System.exit(job2.waitForCompletion(true)? 0:1);
        }
    }
    
}
