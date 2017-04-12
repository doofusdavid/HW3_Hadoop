package cs455.hadoop.q5;


import cs455.hadoop.util.CensusData;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

public class Q5Data implements Writable
{
    private HashMap<String, IntWritable> values;

    public Q5Data()
    {
        values = new LinkedHashMap<>(20);
        values.put("houseValueUnder15k", new IntWritable());
        values.put("houseValue15kto20k", new IntWritable());
        values.put("houseValue20kto25k", new IntWritable());
        values.put("houseValue25kto30k", new IntWritable());
        values.put("houseValue30kto35k", new IntWritable());
        values.put("houseValue35kto40k", new IntWritable());
        values.put("houseValue40kto45k", new IntWritable());
        values.put("houseValue45kto50k", new IntWritable());
        values.put("houseValue50kto60k", new IntWritable());
        values.put("houseValue60kto75k", new IntWritable());
        values.put("houseValue75kto100k", new IntWritable());
        values.put("houseValue100kto125k", new IntWritable());
        values.put("houseValue125kto150k", new IntWritable());
        values.put("houseValue150kto175k", new IntWritable());
        values.put("houseValue175kto200k", new IntWritable());
        values.put("houseValue200kto250k", new IntWritable());
        values.put("houseValue250kto300k", new IntWritable());
        values.put("houseValue300kto400k", new IntWritable());
        values.put("houseValue400kto500k", new IntWritable());
        values.put("houseValue500kAndOver", new IntWritable());
    }

    public HashMap<String, IntWritable> getValues()
    {
        return values;
    }

    public void set(CensusData censusData)
    {
        values.put("houseValueUnder15k", new IntWritable(censusData.getHouseValueUnder15k()));
        values.put("houseValue15kto20k", new IntWritable(censusData.getHouseValue15kto20k()));
        values.put("houseValue20kto25k", new IntWritable(censusData.getHouseValue20kto25k()));
        values.put("houseValue25kto30k", new IntWritable(censusData.getHouseValue25kto30k()));
        values.put("houseValue30kto35k", new IntWritable(censusData.getHouseValue30kto35k()));
        values.put("houseValue35kto40k", new IntWritable(censusData.getHouseValue35kto40k()));
        values.put("houseValue40kto45k", new IntWritable(censusData.getHouseValue40kto45k()));
        values.put("houseValue45kto50k", new IntWritable(censusData.getHouseValue45kto50k()));
        values.put("houseValue50kto60k", new IntWritable(censusData.getHouseValue50kto60k()));
        values.put("houseValue60kto75k", new IntWritable(censusData.getHouseValue60kto75k()));
        values.put("houseValue75kto100k", new IntWritable(censusData.getHouseValue75kto100k()));
        values.put("houseValue100kto125k", new IntWritable(censusData.getHouseValue100kto125k()));
        values.put("houseValue125kto150k", new IntWritable(censusData.getHouseValue125kto150k()));
        values.put("houseValue150kto175k", new IntWritable(censusData.getHouseValue150kto175k()));
        values.put("houseValue175kto200k", new IntWritable(censusData.getHouseValue175kto200k()));
        values.put("houseValue200kto250k", new IntWritable(censusData.getHouseValue200kto250k()));
        values.put("houseValue250kto300k", new IntWritable(censusData.getHouseValue250kto300k()));
        values.put("houseValue300kto400k", new IntWritable(censusData.getHouseValue300kto400k()));
        values.put("houseValue400kto500k", new IntWritable(censusData.getHouseValue400kto500k()));
        values.put("houseValue500kAndOver", new IntWritable(censusData.getHouseValue500kAndOver()));
    }

    /**
     * Implement writable so the data can go between mappers and reducers
     *
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
     * @param mergeData Q5Data containing the values you want to add to this Q5Data
     */
    public void merge(Q5Data mergeData)
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
        String outString = "";

        for (HashMap.Entry<String, IntWritable> entry : values.entrySet())
        {
            outString += String.format("|%-25s: %d", entry.getKey(), entry.getValue().get());
        }
        outString += String.format("|%-25s: %s", "Median", this.median());
        return outString;
    }

    /**
     * Calculate median value of all items in values
     * @return median as a string to be displayed
     */
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
