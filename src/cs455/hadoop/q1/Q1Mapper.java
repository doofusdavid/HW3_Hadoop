package cs455.hadoop.q1;

import cs455.hadoop.util.CensusData;
import cs455.hadoop.util.State;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Write out the state owner and renter occupied values using StateAbbreviation - Rent, and -Own for keys.
 * Also, used Counters to maintain the totals.
 */
public class Q1Mapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        CensusData data = new CensusData(value.toString());
        if (data.getSummaryLevel() == 100)
        {
            context.write(new Text(data.getStateAbbreviation() + " - Rent"), new IntWritable(data.getRenterOccupied()));
            context.write(new Text(data.getStateAbbreviation() + " - Own"), new IntWritable(data.getOwnerOccupied()));
            context.getCounter(State.valueOfAbbreviation(data.getStateAbbreviation())).increment(data.getOwnerOccupied() + data.getRenterOccupied());
        }
    }
}
