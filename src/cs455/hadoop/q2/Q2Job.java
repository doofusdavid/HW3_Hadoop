package cs455.hadoop.q2;


import cs455.hadoop.util.State;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Q2Job
{
    public static void main(String[] args)
    {
        try
        {
            Configuration conf = new Configuration();

            Job job = Job.getInstance(conf, "cdedward Q2 job");
            job.setJarByClass(Q2Job.class);
            job.setMapperClass(Q2Mapper.class);
            job.setCombinerClass(Q2Reducer.class);
            job.setReducerClass(Q2Reducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.waitForCompletion(true);
            Counters counters = job.getCounters();

            FileSystem fs = FileSystem.get(conf);
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(args[1] + "/part-r-00000"))));
            String line = null;
            FSDataOutputStream outputStream = fs.create(new Path(args[1] + "/finalResults.txt"));

            while ((line = reader.readLine()) != null)
            {
                String[] keyval = line.split("\\t");
                String[] keysplit = keyval[0].split("\\s+");
                Counter counter = counters.findCounter(State.valueOfAbbreviation(keysplit[0]));

                double percentage = Float.parseFloat(keyval[1]) / counter.getValue() * 100.0;
                String outputLine = line + String.format(" - Percentage: %.2f%% \n", percentage);
                System.out.println(outputLine);
                outputStream.writeChars(outputLine);
            }
            outputStream.flush();
            outputStream.close();
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
