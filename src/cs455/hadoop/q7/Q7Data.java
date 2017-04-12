package cs455.hadoop.q7;

import cs455.hadoop.util.CensusData;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Q7Data implements Writable
{
    private HashMap<String, IntWritable> values;

    public Q7Data()
    {
        values = new HashMap<>();
        values.put("1", new IntWritable());
        values.put("2", new IntWritable());
        values.put("3", new IntWritable());
        values.put("4", new IntWritable());
        values.put("5", new IntWritable());
        values.put("6", new IntWritable());
        values.put("7", new IntWritable());
        values.put("8", new IntWritable());
        values.put("9", new IntWritable());
    }

    public HashMap<String, IntWritable> getValues()
    {
        return values;
    }

    public void set(CensusData censusData)
    {
        values.put("1", new IntWritable(censusData.getHouse1Room()));
        values.put("2", new IntWritable(censusData.getHouse2Room()));
        values.put("3", new IntWritable(censusData.getHouse3Room()));
        values.put("4", new IntWritable(censusData.getHouse4Room()));
        values.put("5", new IntWritable(censusData.getHouse5Room()));
        values.put("6", new IntWritable(censusData.getHouse6Room()));
        values.put("7", new IntWritable(censusData.getHouse7Room()));
        values.put("8", new IntWritable(censusData.getHouse8Room()));
        values.put("9", new IntWritable(censusData.getHouse9Room()));
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
    public void readFields(DataInput in) throws IOException
    {
        for (HashMap.Entry<String, IntWritable> entry : values.entrySet())
        {
            entry.getValue().readFields(in);
        }

    }

    public void merge(Q7Data mergeData)
    {
        for (HashMap.Entry<String, IntWritable> entry : values.entrySet())
        {
            int val = entry.getValue().get();
            val += mergeData.getValues().get(entry.getKey()).get();
            entry.setValue(new IntWritable(val));
        }
    }

    @Override
    public String toString()
    {
        return super.toString();
    }

    public double average()
    {
        long entries = 0;
        long rooms = 0;
        ArrayList<String> allValues = new ArrayList<>();
        for (Map.Entry<String, IntWritable> entry : values.entrySet())
        {
            for (int i = 0; i < entry.getValue().get(); i++)
            {
                entries += entry.getValue().get();
                rooms += Integer.parseInt(entry.getKey()) * entry.getValue().get();
            }
        }
        double average = (double) rooms / entries;
        return average;
    }
    public String ninetyFifthPercentile()
    {
        ArrayList<String> allValues = new ArrayList<>();
        for (Map.Entry<String, IntWritable> entry : values.entrySet())
        {
            for (int i = 0; i < entry.getValue().get(); i++)
            {
                allValues.add(entry.getKey());
            }
        }

        double count = allValues.size();
        Double placement = (count * .95);
        System.out.println(String.format("count: %f  -  placement: %f  -  size: %d", count, placement, allValues.size()));
        return "\nThe 95th percentile of the average number of rooms per house across all states: " + allValues.get(placement.intValue());

    }
}