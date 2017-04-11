package cs455.hadoop.q6;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Q6Reducer extends Reducer<Text, Q6Data, Text, Q6Data>
{
    @Override
    protected void reduce(Text key, Iterable<Q6Data> values, Context context) throws IOException, InterruptedException
    {
        Q6Data output = new Q6Data();
        for (Q6Data data : values)
        {
            output.merge(data);
        }

    }

}
