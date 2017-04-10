package cs455.hadoop.q6;

import cs455.hadoop.util.CensusData;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;


public class Q6Data implements Writable
{
    private HashMap<String, IntWritable> values;

    public Q6Data()
    {
        values = new LinkedHashMap<>();
        values.put("rentUnder100", new IntWritable());
        values.put("rent100To149", new IntWritable());
        values.put("rent150To199", new IntWritable());
        values.put("rent200To249", new IntWritable());
        values.put("rent250To299", new IntWritable());
        values.put("rent300To349", new IntWritable());
        values.put("rent350To399", new IntWritable());
        values.put("rent400To449", new IntWritable());
        values.put("rent450To499", new IntWritable());
        values.put("rent500To549", new IntWritable());
        values.put("rent550To599", new IntWritable());
        values.put("rent600To649", new IntWritable());
        values.put("rent650To699", new IntWritable());
        values.put("rent700To749", new IntWritable());
        values.put("rent750To999", new IntWritable());
        values.put("rentOver1000", new IntWritable());
        values.put("rentNoCashRent", new IntWritable());
    }

    public HashMap<String, IntWritable> getValues()
    {
        return values;
    }

    public void set(CensusData censusData)
    {
        values.put("rentUnder100", new IntWritable(censusData.getRentUnder100()));
        values.put("rent100To149", new IntWritable(censusData.getRent100To149()));
        values.put("rent150To199", new IntWritable(censusData.getRent150To199()));
        values.put("rent200To249", new IntWritable(censusData.getRent200To249()));
        values.put("rent250To299", new IntWritable(censusData.getRent250To299()));
        values.put("rent300To349", new IntWritable(censusData.getRent300To349()));
        values.put("rent350To399", new IntWritable(censusData.getRent350To399()));
        values.put("rent400To449", new IntWritable(censusData.getRent400To449()));
        values.put("rent450To499", new IntWritable(censusData.getRent450To499()));
        values.put("rent500To549", new IntWritable(censusData.getRent500To549()));
        values.put("rent550To599", new IntWritable(censusData.getRent550To599()));
        values.put("rent600To649", new IntWritable(censusData.getRent600To649()));
        values.put("rent650To699", new IntWritable(censusData.getRent650To699()));
        values.put("rent700To749", new IntWritable(censusData.getRent700To749()));
        values.put("rent750To999", new IntWritable(censusData.getRent750To999()));
        values.put("rentOver1000", new IntWritable(censusData.getRentOver1000()));
        values.put("rentNoCashRent", new IntWritable(censusData.getRentNoCashRent()));
    }

    @Override
    public String toString()
    {
        String outString = "";

        for (HashMap.Entry<String, IntWritable> entry : values.entrySet())
        {
            outString += String.format("|%-25s: %d", entry.getKey(), entry.getValue().get());
        }
        outString += String.format("|%-25s: %s", "Median", this.median());
        return outString;
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
        for (HashMap.Entry<String, IntWritable> entry : values.entrySet())
        {
            entry.getValue().write((out));
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException
    {

    }

    public void merge(Q6Data mergeData)
    {
        for (HashMap.Entry<String, IntWritable> entry : values.entrySet())
        {
            int val = entry.getValue().get();
            val += mergeData.getValues().get(entry.getKey()).get();
            entry.setValue(new IntWritable(val));
        }

    }

    public String median()
    {
        ArrayList<String> allValues = new ArrayList<>();
        for (HashMap.Entry<String, IntWritable> entry : values.entrySet())
        {
            for (int i = 0; i < entry.getValue().get(); i++)
            {
                allValues.add(entry.getKey());
            }
        }

        int middle = allValues.size() / 2;
        try
        {

            if (allValues.size() % 2 == 1)
            {
                return allValues.get(middle);
            } else
            {
                if (!allValues.get(middle - 1).equals(allValues.get(middle)))
                {
                    return "\nMedian between: " + allValues.get(middle - 1) + " and " + allValues.get(middle);
                } else
                {
                    return allValues.get(middle);
                }
            }
        }
        catch (Exception e)
        {
            return "middle: " + e.getMessage();
        }
    }

}
