package cs455.hadoop.q8;

import cs455.hadoop.util.CensusData;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Simple data, so going with a pipe delimited string
 */
public class Q8Mapper extends Mapper<LongWritable, Text, Text, Text>
{

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        CensusData data = new CensusData(value.toString());
        if (data.getSummaryLevel() == 100)
        {
            context.write(new Text(String.format("%s|Over80|Total", data.getStateAbbreviation())), new Text(String.format("%s|%s", data.getAge85andOver(), data.getPersons())));
        }
    }
}
