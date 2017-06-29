/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package contributingfactormalefemale;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author deeptipundlik
 */
public class ContributingFactorMaleFemale {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "VehicleInformation");
        job1.setJarByClass(ContributingFactorMaleFemale.class);
        MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class,ContributingFactor_Mapper.class);
        MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class,IndividualInformation_Mapper.class);
        job1.setReducerClass(ContributingFactor_Reducer.class);
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job1, new Path(args[2]));
        boolean jobComplete = job1.waitForCompletion(true);
        
        if(jobComplete){
        
            Configuration conf1 = new Configuration();
            Job job2 = Job.getInstance(conf1, "CountJob");
            job2.setJarByClass(ContributingFactorMaleFemale.class);
            job2.setMapperClass(Count_Mapper.class);
            job2.setReducerClass(Count_Reducer.class);
            job2.setOutputKeyClass(CompositeKeyWritable.class);
            job2.setOutputValueClass(IntWritable.class);
            
            FileInputFormat.addInputPath(job2, new Path(args[2]));
            FileOutputFormat.setOutputPath(job2, new Path(args[3]));
            boolean value = job2.waitForCompletion(true);
            
            if(value){
                Configuration conf2 = new Configuration();
                Job job3 = Job.getInstance(conf2, "PartitionJob");
                job3.setJarByClass(ContributingFactorMaleFemale.class);
                job3.setMapperClass(Partition_Mapper.class);
                job3.setPartitionerClass(ContributingFactorGenderPartitioner.class);
                job3.setOutputKeyClass(Text.class);
                job3.setNumReduceTasks(2);
                job3.setOutputValueClass(Text.class);
                FileInputFormat.addInputPath(job3, new Path(args[3]));
                FileOutputFormat.setOutputPath(job3, new Path(args[4]));
                System.exit(job3.waitForCompletion(true)? 0:1);
            
            }
        }
    }
    
}
