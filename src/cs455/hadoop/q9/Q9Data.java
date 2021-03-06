package cs455.hadoop.q9;

import cs455.hadoop.util.CensusData;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.TreeMap;

/**
 * Data class implementing writable for Q3, to avoid large pipe-delimited strings
 */

public class Q9Data implements Writable
{
    private TreeMap<String, IntWritable> values;

    /**
     * initialize everything as blank, otherwise Hadoop will break it later
     */
    public Q9Data()
    {
        values = new TreeMap<>();
        values.put("0.0 Total Population", new IntWritable());
        values.put("1.1 Male", new IntWritable());
        values.put("1.2 Female", new IntWritable());
        values.put("2.1 Under 18 years old", new IntWritable());
        values.put("2.2 18 to 44 years old", new IntWritable());
        values.put("2.3 45 to 64 years old", new IntWritable());
        values.put("2.4 65 years and older", new IntWritable());
    }

    public TreeMap<String, IntWritable> getValues()
    {
        return values;
    }

    /**
     * set the TreeMap values by adding the relevant CensusData items
     *
     * @param censusData the line of census data from HDFS
     */
    public void set(CensusData censusData)
    {
        values.put("0.0 Total Population", new IntWritable(censusData.getPersons()));
        values.put("1.1 Male", new IntWritable(censusData.getMale()));
        values.put("1.2 Female", new IntWritable(censusData.getFemale()));

        int under18 = censusData.getAgeUnder1() +
                censusData.getAgeBetween1and2() +
                censusData.getAgeBetween3and4() +
                censusData.getAge5() +
                censusData.getAge6() +
                censusData.getAgeBetween7and9() +
                censusData.getAgeBetween10and11() +
                censusData.getAgeBetween12and13() +
                censusData.getAge14() +
                censusData.getAge15() +
                censusData.getAge16() +
                censusData.getAge17();

        values.put("2.1 Under 18 years old", new IntWritable(under18));

        int eighteenTo44 = censusData.getAge18() +
                censusData.getAge19() +
                censusData.getAge20() +
                censusData.getAge21() +
                censusData.getAgeBetween22and24() +
                censusData.getAgeBetween25and29() +
                censusData.getAgeBetween30and34() +
                censusData.getAgeBetween35and39() +
                censusData.getAgeBetween40and44();

        values.put("2.2 18 to 44 years old", new IntWritable(eighteenTo44));

        int fortyFiveTo64 = censusData.getAgeBetween45and49() +
                censusData.getAgeBetween50and54() +
                censusData.getAgeBetween55and59() +
                censusData.getAgeBetween60and61() +
                censusData.getAgeBetween62and64();

        values.put("2.3 45 to 64 years old", new IntWritable(fortyFiveTo64));

        int sixtyFiveAndOlder = censusData.getAgeBetween65and69() +
                censusData.getAgeBetween70and74() +
                censusData.getAgeBetween75and79() +
                censusData.getAgeBetween80and84() +
                censusData.getAge85andOver();
        values.put("2.4 65 years and older", new IntWritable(sixtyFiveAndOlder));
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
            outString += String.format("|%-30s: %d", entry.getKey(), entry.getValue().get());
        }
        return outString;
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
    public void merge(Q9Data mergeData)
    {
        for (HashMap.Entry<String, IntWritable> entry : values.entrySet())
        {
            int val = entry.getValue().get();
            val += mergeData.getValues().get(entry.getKey()).get();
            entry.setValue(new IntWritable(val));
        }
    }
}
