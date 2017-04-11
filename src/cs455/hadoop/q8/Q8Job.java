package cs455.hadoop.q8;

import cs455.hadoop.util.MapUtil;
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
import java.util.LinkedHashMap;
import java.util.Map;

public class Q8Job
{

    public static void main(String[] args)
    {
        try
        {
            Configuration conf = new Configuration();

            Job job = Job.getInstance(conf, "cdedward Q8 job");
            job.setJarByClass(Q8Job.class);
            job.setMapperClass(Q8Mapper.class);
            job.setCombinerClass(Q8Reducer.class);
            job.setReducerClass(Q8Reducer.class);

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

            Map<String, Double> statePercentages = new LinkedHashMap<>();
            while ((line = reader.readLine()) != null)
            {
                String[] keyval = line.split("\\t");
                String[] keysplit = keyval[0].split("\\|");
                String[] valsplit = keyval[1].split("\\|");

                String state = keysplit[0];
                double percent = Double.parseDouble(valsplit[0]) / Double.parseDouble(valsplit[1]) * 100.0;
                statePercentages.put(state, percent);
            }
            statePercentages = MapUtil.sortByValue(statePercentages);
            for (Map.Entry<String, Double> state : statePercentages.entrySet())
            {
                outputStream.writeChars(String.format("State: %s: Percent Over 85: %.2f\n", State.valueOfAbbreviation(state.getKey()), state.getValue()));
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
