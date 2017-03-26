package cs455.hadoop.q4;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Q4Reducer extends Reducer<Text, Text, Text, Text>
{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
        int urbanInsideUrbanized = 0;
        int urbanOutsideUrbanized = 0;
        int rural = 0;
        int undefined = 0;

        for (Text val : values)
        {
            String[] value = val.toString().split("\\|");
            urbanInsideUrbanized += Integer.parseInt(value[0]);
            urbanOutsideUrbanized += Integer.parseInt(value[1]);
            rural += Integer.parseInt(value[2]);
            undefined += Integer.parseInt(value[3]);
        }
        context.write(key, new Text(String.format("%d|%d|%d|%d", urbanInsideUrbanized, urbanOutsideUrbanized, rural, undefined)));
    }
}
