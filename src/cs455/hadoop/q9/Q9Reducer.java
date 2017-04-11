package cs455.hadoop.q9;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class Q9Reducer extends Reducer<Text, Q9Data, Text, Q9Data>
{
    @Override
    protected void reduce(Text key, Iterable<Q9Data> values, Context context) throws IOException, InterruptedException
    {
        Q9Data output = new Q9Data();
        for (Q9Data data : values)
        {
            output.merge(data);
        }
        context.write(key, output);
    }
}
