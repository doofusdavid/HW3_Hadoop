package cs455.hadoop.q2;

import cs455.hadoop.util.CensusData;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class Q2Mapper extends Mapper<LongWritable, Text, Text, Text>
{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        CensusData data = new CensusData(value.toString());
        if (data.getSummaryLevel() == 100)
        {
            context.write(new Text(data.getStateAbbreviation() + "|MNM|FNM|MTOT|FTOT"), new Text(String.format("%s|%s|%s|%s", data.getMaleNeverMarried(), data.getFemaleNeverMarried(), data.getMale(), data.getFemale())));
        }
    }
}
