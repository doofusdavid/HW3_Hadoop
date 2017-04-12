package cs455.hadoop.q3;


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
public class Q3Data implements Writable
{
    // TreeMap so that everything stays sorted
    private TreeMap<String, IntWritable> values;

    /**
     * initialize everything as blank, otherwise Hadoop will break it later
     */
    public Q3Data()
    {
        values = new TreeMap<>();
        values.put("Hispanic Male 18 years and under", new IntWritable());
        values.put("Hispanic Male 19 to 24 years old", new IntWritable());
        values.put("Hispanic Male 30 to 39 years old", new IntWritable());
        values.put("Hispanic Female 18 years and under", new IntWritable());
        values.put("Hispanic Female 19 to 24 years old", new IntWritable());
        values.put("Hispanic Female 30 to 39 years old", new IntWritable());
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
        int maleYoung = censusData.getHispanicMaleAgeUnder1()
                + censusData.getHispanicMaleAgeBetween1and2()
                + censusData.getHispanicMaleAgeBetween3and4()
                + censusData.getHispanicMaleAge5()
                + censusData.getHispanicMaleAge6()
                + censusData.getHispanicMaleAgeBetween7and9()
                + censusData.getHispanicMaleAgeBetween10and11()
                + censusData.getHispanicMaleAgeBetween12and13()
                + censusData.getHispanicMaleAge14()
                + censusData.getHispanicMaleAge15()
                + censusData.getHispanicMaleAge16()
                + censusData.getHispanicMaleAge17()
                + censusData.getHispanicMaleAge18();
        values.put("Hispanic Male 18 years and under", new IntWritable(maleYoung));

        int maleMiddle = censusData.getHispanicMaleAge19() +
                censusData.getHispanicMaleAge20() +
                censusData.getHispanicMaleAge21() +
                censusData.getHispanicMaleAgeBetween22and24();
        values.put("Hispanic Male 19 to 24 years old", new IntWritable(maleMiddle));

        int maleOlder = censusData.getHispanicMaleAgeBetween30and34() +
                censusData.getHispanicMaleAgeBetween35and39();

        values.put("Hispanic Male 30 to 39 years old", new IntWritable(maleOlder));

        int femaleYoung = censusData.getHispanicFemaleAgeUnder1()
                + censusData.getHispanicFemaleAgeBetween1and2()
                + censusData.getHispanicFemaleAgeBetween3and4()
                + censusData.getHispanicFemaleAge5()
                + censusData.getHispanicFemaleAge6()
                + censusData.getHispanicFemaleAgeBetween7and9()
                + censusData.getHispanicFemaleAgeBetween10and11()
                + censusData.getHispanicFemaleAgeBetween12and13()
                + censusData.getHispanicFemaleAge14()
                + censusData.getHispanicFemaleAge15()
                + censusData.getHispanicFemaleAge16()
                + censusData.getHispanicFemaleAge17()
                + censusData.getHispanicFemaleAge18();
        values.put("Hispanic Female 18 years and under", new IntWritable(femaleYoung));

        int femaleMiddle = censusData.getHispanicFemaleAge19() +
                censusData.getHispanicFemaleAge20() +
                censusData.getHispanicFemaleAge21() +
                censusData.getHispanicFemaleAgeBetween22and24();
        values.put("Hispanic Female 19 to 24 years old", new IntWritable(femaleMiddle));

        int femaleOlder = censusData.getHispanicFemaleAgeBetween30and34() +
                censusData.getHispanicFemaleAgeBetween35and39();

        values.put("Hispanic Female 30 to 39 years old", new IntWritable(femaleOlder));

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
            outString += String.format("|%-40s: %d", entry.getKey(), entry.getValue().get());
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
    public void merge(Q3Data mergeData)
    {
        for (HashMap.Entry<String, IntWritable> entry : values.entrySet())
        {
            int val = entry.getValue().get();
            val += mergeData.getValues().get(entry.getKey()).get();
            entry.setValue(new IntWritable(val));
        }
    }
}
