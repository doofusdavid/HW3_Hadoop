package cs455.hadoop.q5;


import cs455.hadoop.util.CensusData;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Q5Mapper extends Mapper<LongWritable, Text, Text, Q5Data>
{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        CensusData censusData = new CensusData(value.toString());
        if (censusData.getSummaryLevel() == 100)
        {
            Q5Data q5Data = new Q5Data();
            q5Data.set(censusData);
            context.write(new Text(censusData.getStateAbbreviation()), q5Data);
            System.out.println(q5Data.toString());
        }
    }
}
