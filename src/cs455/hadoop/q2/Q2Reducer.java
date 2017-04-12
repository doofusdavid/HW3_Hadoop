package cs455.hadoop.q2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Increment the values from the Mapper.  Need to split them out from the pipe delimited string, and reform them
 * into another pipe delimited string before writing them.
 */
public class Q2Reducer extends Reducer<Text, Text, Text, Text>
{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
        int maleNeverMarriedCount = 0;
        int femaleNeverMarriedCount = 0;
        int maleTotalCount = 0;
        int femaleTotalCount = 0;

        for (Text val : values)
        {
            String[] value = val.toString().split("\\|");
            maleNeverMarriedCount += Integer.parseInt(value[0]);
            femaleNeverMarriedCount += Integer.parseInt(value[1]);
            maleTotalCount += Integer.parseInt(value[2]);
            femaleTotalCount += Integer.parseInt(value[3]);

        }
        context.write(key, new Text(String.format("%s|%s|%s|%s", maleNeverMarriedCount, femaleNeverMarriedCount, maleTotalCount, femaleTotalCount)));
    }
}
