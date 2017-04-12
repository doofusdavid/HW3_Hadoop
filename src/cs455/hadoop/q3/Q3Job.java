package cs455.hadoop.q3;


import cs455.hadoop.util.State;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Q3Job
{
    public static void main(String[] args)
    {
        try
        {
            Configuration conf = new Configuration();

            Job job = Job.getInstance(conf, "cdedward Q3 Job");
            job.setJarByClass(Q3Job.class);
            job.setMapperClass(Q3Mapper.class);
            job.setCombinerClass(Q3Reducer.class);
            job.setReducerClass(Q3Reducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Q3Data.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Q3Data.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.waitForCompletion(true);

            // read in the results file, save to new file
            FileSystem fs = FileSystem.get(conf);
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(args[1] + "/part-r-00000"))));
            String line = null;
            FSDataOutputStream outputStream = fs.create(new Path(args[1] + "/finalResults.txt"));

            // Iterate through results, save them in a formatted way
            while ((line = reader.readLine()) != null)
            {
                String[] keyval = line.split("\\t");
                String outputLine = String.format("%s\n_______________________\n%s\n\n\n", State.valueOfAbbreviation(keyval[0]), keyval[1].replace("|", "\n"));
                outputStream.writeChars(outputLine);

            }
            outputStream.flush();
            outputStream.close();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
        catch (ClassNotFoundException e)
        {
            e.printStackTrace();
        }

    }
}
