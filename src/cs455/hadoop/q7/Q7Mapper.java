package cs455.hadoop.q7;

import cs455.hadoop.util.CensusData;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Q7Mapper extends Mapper<LongWritable, Text, Text, Q7Data>
{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        CensusData censusData = new CensusData(value.toString());
        if (censusData.getSummaryLevel() == 100)
        {
            Q7Data q7Data = new Q7Data();
            q7Data.set(censusData);

            // Key kept the same since this is across all states
            context.write(new Text("allStates"), q7Data);
        }
    }
}
