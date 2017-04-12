package cs455.hadoop.q9;

import cs455.hadoop.util.CensusData;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Q9Mapper extends Mapper<LongWritable, Text, Text, Q9Data>
{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        // We don't care about states, we want all data
        CensusData censusData = new CensusData(value.toString());
        if (censusData.getSummaryLevel() == 100)
        {
            Q9Data q6Data = new Q9Data();
            q6Data.set(censusData);
            context.write(new Text("United States: "), q6Data);
        }

    }
}
