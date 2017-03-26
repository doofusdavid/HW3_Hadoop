package cs455.hadoop.q3;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Q3Mapper extends Mapper<LongWritable, Text, Text, Text>
{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        super.map(key, value, context);
    }
}
