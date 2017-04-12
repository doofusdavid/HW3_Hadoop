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

/**
 * Data class implementing writable for Q3, to avoid large pipe-delimited strings
 */
public class Q7Data implements Writable
{
    private HashMap<String, IntWritable> values;

    /**
     * initialize everything as blank, otherwise Hadoop will break it later
     */
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

    /**
     * set the HashMap values by adding the relevant CensusData items
     *
     * @param censusData the line of census data from HDFS
     */
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

    /**
     * Implement writable so the data can go between mappers and reducers
     * @param out the DataOutput to write to
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException
    {
        for (HashMap.Entry<String, IntWritable> entry : values.entrySet())
        {
            entry.getValue().write((out));
        }
    }

    /**
     * Implement readFields to hydrate from Hadoop
     * @param in DataInput to read from
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException
    {
        for (HashMap.Entry<String, IntWritable> entry : values.entrySet())
        {
            entry.getValue().readFields(in);
        }

    }

    /**
     * Merge iterates through the values and adds the values passed in.  Used in Reduce.
     * @param mergeData Q3Data containing the values you want to add to this Q3Data
     */
    public void merge(Q7Data mergeData)
    {
        for (HashMap.Entry<String, IntWritable> entry : values.entrySet())
        {
            int val = entry.getValue().get();
            val += mergeData.getValues().get(entry.getKey()).get();
            entry.setValue(new IntWritable(val));
        }
    }

    /**
     * Output pipe delimited string for final work in Q3Job
     * @return pipe delimited string of Map values
     */
    @Override
    public String toString()
    {
        return super.toString();
    }

    /**
     * Return the average of all the items in the map
     * @return
     */
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
}