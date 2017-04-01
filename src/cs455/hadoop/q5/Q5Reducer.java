package cs455.hadoop.q5;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Q5Reducer extends Reducer<Text, Q5Data, Text, Q5Data>
{
    @Override
    protected void reduce(Text key, Iterable<Q5Data> values, Context context) throws IOException, InterruptedException
    {
        Q5Data output = new Q5Data();
        for (Q5Data data : values)
        {
            output.merge(data);
        }

        context.write(key, output);
    }
}
