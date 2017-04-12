package cs455.hadoop.q7;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Q7Reducer extends Reducer<Text, Q7Data, Text, Double>
{
    @Override
    protected void reduce(Text key, Iterable<Q7Data> values, Context context) throws IOException, InterruptedException
    {
        Q7Data output = new Q7Data();
        for (Q7Data data : values)
        {
            output.merge(data);
        }
        context.write(key, output.average());
    }
}
