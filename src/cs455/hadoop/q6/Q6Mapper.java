package cs455.hadoop.q6;


import cs455.hadoop.util.CensusData;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Q6Mapper extends Mapper<LongWritable, Text, Text, Q6Data>
{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        CensusData censusData = new CensusData(value.toString());
        if (censusData.getSummaryLevel() == 100)
        {

        }
    }
}
