package cs455.hadoop.q8;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Q8Reducer extends Reducer<Text, Text, Text, Text>
{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
        int over85Count = 0;
        int totalPersonCount = 0;

        for (Text val : values)
        {
            String[] value = val.toString().split("\\|");
            over85Count += Integer.parseInt(value[0]);
            totalPersonCount += Integer.parseInt(value[1]);
        }

        context.write(key, new Text(String.format("%s|%s", over85Count, totalPersonCount)));
    }
}
