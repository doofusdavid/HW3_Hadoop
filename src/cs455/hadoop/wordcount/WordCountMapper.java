package cs455.hadoop.wordcount;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Reimplement sample code, normalize text, remove punctuation, and make everything lowercase.
 * Also, used to test integration with IntelliJ
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        String[] results = value.toString().split("\\s");

        for (String result : results)
        {
            context.write(new Text(processWord(result)), new IntWritable(1));
        }
    }

    private String processWord(String result)
    {
        return result.replaceAll("[^a-zA-Z\\s]", "").toLowerCase();
    }
}
