/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package byboroughs;

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
public class ByBoroughs {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"AccidentsByBoroughs");
        job.setJarByClass(ByBoroughs.class);
        job.setMapperClass(ByBoroughs_Mapper.class);
        job.setReducerClass(ByBoroughs_Reducer.class);
        job.setOutputKeyClass(CompositeKeyYearBorough.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true)?0:1);
             
    }
    
}
