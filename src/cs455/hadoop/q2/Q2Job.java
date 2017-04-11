package cs455.hadoop.q2;


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
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.waitForCompletion(true);

            FileSystem fs = FileSystem.get(conf);
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(args[1] + "/part-r-00000"))));
            String line = null;
            FSDataOutputStream outputStream = fs.create(new Path(args[1] + "/finalResults.txt"));

            while ((line = reader.readLine()) != null)
            {
                String[] keyval = line.split("\\t");
                String[] keysplit = keyval[0].split("\\|");
                String[] valsplit = keyval[1].split("\\|");

                double malePercent = Double.parseDouble(valsplit[0]) / Double.parseDouble(valsplit[2]) * 100.0;
                double femalePercent = Double.parseDouble(valsplit[1]) / Double.parseDouble(valsplit[3]) * 100.0;

                String outputLine = String.format("%s - Male Unmarried: %.2f%% - Male Total: %s - Female Unmarried: %.2f%% - Female total: %s\n", State.valueOfAbbreviation(keysplit[0]), malePercent, valsplit[2], femalePercent, valsplit[3]);
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
