package cs455.hadoop.q7;

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
import java.util.HashMap;
import java.util.Map;

public class Q7Job
{
    public static void main(String[] args)
    {
        try
        {
            Configuration conf = new Configuration();

            Job job = Job.getInstance(conf, "cdedward Q7 Job");
            job.setJarByClass(Q7Job.class);
            job.setMapperClass(Q7Mapper.class);
            job.setCombinerClass(Q7Combiner.class);
            job.setReducerClass(Q7Reducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Q7Data.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Double.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.waitForCompletion(true);

            FileSystem fs = FileSystem.get(conf);
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(args[1] + "/part-r-00000"))));
            String line = null;
            FSDataOutputStream outputStream = fs.create(new Path(args[1] + "/finalResults.txt"));

            Map<String, Double> results = new HashMap<>();
            while ((line = reader.readLine()) != null)
            {
                String[] keyval = line.split("\\t");
                results.put(keyval[0], Double.parseDouble(keyval[1]));
            }
            results = MapUtil.sortByValue(results);
            String outputLine;

            double sum = 0.0;
            for (Map.Entry<String, Double> result : results.entrySet())
            {
                sum += result.getValue();
                outputLine = String.format("Average Rooms: %s: %f\n", State.valueOfAbbreviation(result.getKey()), result.getValue());
                outputStream.writeChars(outputLine);
            }

            double percentileIndex = sum * 0.95;

            double runningSum = 0;

            for (Map.Entry<String, Double> result : results.entrySet())
            {
                runningSum += result.getValue();

                if (runningSum >= percentileIndex)
                {
                    outputLine = String.format("95th Percentile Average Number of Rooms - State: %s  Rooms: %.2f\n", State.valueOfAbbreviation(result.getKey()), result.getValue());
                    outputStream.writeChars(outputLine);

                    outputStream.flush();
                    outputStream.close();
                    break;
                }
            }
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
