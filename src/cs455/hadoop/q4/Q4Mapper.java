package cs455.hadoop.q4;


import cs455.hadoop.util.CensusData;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Q4Mapper extends Mapper<LongWritable, Text, Text, Text>
{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        CensusData data = new CensusData(value.toString());
        if (data.getSummaryLevel() == 100)
        {
            context.write(new Text(data.getStateAbbreviation() + "|URBIN|URBOUT|RUR|ND"), new Text(String.format("%d|%d|%d|%d", data.getUrbanInsideUrbanizedArea(), data.getUrbanOutsideUrbanizedArea(), data.getRural(), data.getUrbanRuralNotDefined())));
        }
    }
}
