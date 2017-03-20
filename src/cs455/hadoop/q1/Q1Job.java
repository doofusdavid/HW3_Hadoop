package cs455.hadoop.q1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Q1Job
{
    public static void main(String[] args)
    {
        try
        {
            Configuration conf = new Configuration();

            Job job = Job.getInstance(conf, "cdedward Q1 job");
            job.setJarByClass(Q1Job.class);
            job.setMapperClass(Q1Mapper.class);
            job.setCombinerClass(Q1Reducer.class);
            job.setReducerClass(Q1Reducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            // Block until the job is completed
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
        catch (IOException e)
        {
            System.err.println(e.getMessage());
        }
        catch (InterruptedException e)
        {
            System.err.println(e.getMessage());
        }
        catch (ClassNotFoundException e)
        {
            System.err.println(e.getMessage());
        }
    }
}
