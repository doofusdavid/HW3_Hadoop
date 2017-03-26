package cs455.hadoop.q4;

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


public class Q4Job
{

    public static void main(String[] args)
    {

        try
        {
            Configuration conf = new Configuration();

            Job job = Job.getInstance(conf, "cdedward Q4 job");
            job.setJarByClass(Q4Job.class);
            job.setMapperClass(Q4Mapper.class);
            job.setCombinerClass(Q4Reducer.class);
            job.setReducerClass(Q4Reducer.class);

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
                double total = Double.parseDouble(valsplit[0]) + Double.parseDouble(valsplit[1]) + Double.parseDouble(valsplit[2]) + Double.parseDouble(valsplit[3]);

                double urbanInsidePercent = Double.parseDouble(valsplit[0]) / total * 100.0;
                double urbanOutsidePercent = Double.parseDouble(valsplit[1]) / total * 100.0;
                double ruralPercent = Double.parseDouble(valsplit[2]) / total * 100.0;
                double undefinedPercent = Double.parseDouble(valsplit[3]) / total * 100.0;

                outputStream.writeChars(String.format("%s - Urban vs Rural population\n", State.valueOfAbbreviation(keysplit[0]).toString()));
                outputStream.writeChars("\t\tQuantity\tPercentage\n");
                outputStream.writeChars(String.format("Urban, Inside:\t%s\t%.2f%%\n", valsplit[0], urbanInsidePercent));
                outputStream.writeChars(String.format("Urban, Outside:\t%s\t%.2f%%\n", valsplit[1], urbanOutsidePercent));
                outputStream.writeChars(String.format("Rural:\t\t%s\t%.2f%%\n", valsplit[2], ruralPercent));
                outputStream.writeChars(String.format("Undefined:\t%s\t%.2f%%\n", valsplit[3], undefinedPercent));
                outputStream.writeChars(String.format("Total:\t\t%d\t%.2f%%\n", total, 100.0));
                outputStream.writeChars("----------------------------------------------------------------\n");
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
