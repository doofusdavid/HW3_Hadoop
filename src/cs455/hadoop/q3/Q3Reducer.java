package cs455.hadoop.q3;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Q3Reducer extends Reducer<Text, Q3Data, Text, Q3Data>
{
    @Override
    protected void reduce(Text key, Iterable<Q3Data> values, Context context) throws IOException, InterruptedException
    {
        Q3Data output = new Q3Data();
        for (Q3Data data : values)
        {
            output.merge(data);
        }

        context.write(key, output);
    }
}
