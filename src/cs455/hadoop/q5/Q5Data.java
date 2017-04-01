package cs455.hadoop.q5;


import cs455.hadoop.util.CensusData;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Q5Data implements Writable
{
    IntWritable houseValueUnder15k,
            houseValue15kto20k,
            houseValue20kto25k,
            houseValue25kto30k,
            houseValue30kto35k,
            houseValue35kto40k,
            houseValue40kto45k,
            houseValue45kto50k,
            houseValue50kto60k,
            houseValue60kto75k,
            houseValue75kto100k,
            houseValue100kto125k,
            houseValue125kto150k,
            houseValue150kto175k,
            houseValue175kto200k,
            houseValue200kto250k,
            houseValue250kto300k,
            houseValue300kto400k,
            houseValue400kto500k,
            houseValue500kAndOver;

    public Q5Data()
    {
        houseValueUnder15k = new IntWritable();
        houseValue15kto20k = new IntWritable();
        houseValue20kto25k = new IntWritable();
        houseValue25kto30k = new IntWritable();
        houseValue30kto35k = new IntWritable();
        houseValue35kto40k = new IntWritable();
        houseValue40kto45k = new IntWritable();
        houseValue45kto50k = new IntWritable();
        houseValue50kto60k = new IntWritable();
        houseValue60kto75k = new IntWritable();
        houseValue75kto100k = new IntWritable();
        houseValue100kto125k = new IntWritable();
        houseValue125kto150k = new IntWritable();
        houseValue150kto175k = new IntWritable();
        houseValue175kto200k = new IntWritable();
        houseValue200kto250k = new IntWritable();
        houseValue250kto300k = new IntWritable();
        houseValue300kto400k = new IntWritable();
        houseValue400kto500k = new IntWritable();
        houseValue500kAndOver = new IntWritable();
    }

    public void set(CensusData censusData)
    {
        houseValueUnder15k = new IntWritable(censusData.getHouseValueUnder15k());
        houseValue15kto20k = new IntWritable(censusData.getHouseValue15kto20k());
        houseValue20kto25k = new IntWritable(censusData.getHouseValue20kto25k());
        houseValue25kto30k = new IntWritable(censusData.getHouseValue25kto30k());
        houseValue30kto35k = new IntWritable(censusData.getHouseValue30kto35k());
        houseValue35kto40k = new IntWritable(censusData.getHouseValue35kto40k());
        houseValue40kto45k = new IntWritable(censusData.getHouseValue40kto45k());
        houseValue45kto50k = new IntWritable(censusData.getHouseValue45kto50k());
        houseValue50kto60k = new IntWritable(censusData.getHouseValue50kto60k());
        houseValue60kto75k = new IntWritable(censusData.getHouseValue60kto75k());
        houseValue75kto100k = new IntWritable(censusData.getHouseValue75kto100k());
        houseValue100kto125k = new IntWritable(censusData.getHouseValue100kto125k());
        houseValue125kto150k = new IntWritable(censusData.getHouseValue125kto150k());
        houseValue150kto175k = new IntWritable(censusData.getHouseValue150kto175k());
        houseValue175kto200k = new IntWritable(censusData.getHouseValue175kto200k());
        houseValue200kto250k = new IntWritable(censusData.getHouseValue200kto250k());
        houseValue250kto300k = new IntWritable(censusData.getHouseValue250kto300k());
        houseValue300kto400k = new IntWritable(censusData.getHouseValue300kto400k());
        houseValue400kto500k = new IntWritable(censusData.getHouseValue400kto500k());
        houseValue500kAndOver = new IntWritable(censusData.getHouseValue500kAndOver());
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
        houseValueUnder15k.write(out);
        houseValue15kto20k.write(out);
        houseValue20kto25k.write(out);
        houseValue25kto30k.write(out);
        houseValue30kto35k.write(out);
        houseValue35kto40k.write(out);
        houseValue40kto45k.write(out);
        houseValue45kto50k.write(out);
        houseValue50kto60k.write(out);
        houseValue60kto75k.write(out);
        houseValue75kto100k.write(out);
        houseValue100kto125k.write(out);
        houseValue125kto150k.write(out);
        houseValue150kto175k.write(out);
        houseValue175kto200k.write(out);
        houseValue200kto250k.write(out);
        houseValue250kto300k.write(out);
        houseValue300kto400k.write(out);
        houseValue400kto500k.write(out);
        houseValue500kAndOver.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException
    {
        houseValueUnder15k.readFields(in);
        houseValue15kto20k.readFields(in);
        houseValue20kto25k.readFields(in);
        houseValue25kto30k.readFields(in);
        houseValue30kto35k.readFields(in);
        houseValue35kto40k.readFields(in);
        houseValue40kto45k.readFields(in);
        houseValue45kto50k.readFields(in);
        houseValue50kto60k.readFields(in);
        houseValue60kto75k.readFields(in);
        houseValue75kto100k.readFields(in);
        houseValue100kto125k.readFields(in);
        houseValue125kto150k.readFields(in);
        houseValue150kto175k.readFields(in);
        houseValue175kto200k.readFields(in);
        houseValue200kto250k.readFields(in);
        houseValue250kto300k.readFields(in);
        houseValue300kto400k.readFields(in);
        houseValue400kto500k.readFields(in);
        houseValue500kAndOver.readFields(in);
    }

    public void merge(Q5Data mergeData)
    {

        houseValueUnder15k.set(houseValueUnder15k.get() + mergeData.houseValueUnder15k.get());
        houseValue15kto20k.set(houseValue15kto20k.get() + mergeData.houseValue15kto20k.get());
        houseValue20kto25k.set(houseValue20kto25k.get() + mergeData.houseValue20kto25k.get());
        houseValue25kto30k.set(houseValue25kto30k.get() + mergeData.houseValue25kto30k.get());
        houseValue30kto35k.set(houseValue30kto35k.get() + mergeData.houseValue30kto35k.get());
        houseValue35kto40k.set(houseValue35kto40k.get() + mergeData.houseValue35kto40k.get());
        houseValue40kto45k.set(houseValue40kto45k.get() + mergeData.houseValue40kto45k.get());
        houseValue45kto50k.set(houseValue45kto50k.get() + mergeData.houseValue45kto50k.get());
        houseValue50kto60k.set(houseValue50kto60k.get() + mergeData.houseValue50kto60k.get());
        houseValue60kto75k.set(houseValue60kto75k.get() + mergeData.houseValue60kto75k.get());
        houseValue75kto100k.set(houseValue75kto100k.get() + mergeData.houseValue75kto100k.get());
        houseValue100kto125k.set(houseValue100kto125k.get() + mergeData.houseValue100kto125k.get());
        houseValue125kto150k.set(houseValue125kto150k.get() + mergeData.houseValue125kto150k.get());
        houseValue150kto175k.set(houseValue150kto175k.get() + mergeData.houseValue150kto175k.get());
        houseValue175kto200k.set(houseValue175kto200k.get() + mergeData.houseValue175kto200k.get());
        houseValue200kto250k.set(houseValue200kto250k.get() + mergeData.houseValue200kto250k.get());
        houseValue250kto300k.set(houseValue250kto300k.get() + mergeData.houseValue250kto300k.get());
        houseValue300kto400k.set(houseValue300kto400k.get() + mergeData.houseValue300kto400k.get());
        houseValue400kto500k.set(houseValue400kto500k.get() + mergeData.houseValue400kto500k.get());
        houseValue500kAndOver.set(houseValue500kAndOver.get() + mergeData.houseValue500kAndOver.get());
    }

    @Override
    public String toString()
    {
        String outString = "";
        outString += "houseValueUnder15k   : " + houseValueUnder15k;
        outString += "|houseValue15kto20k   : " + houseValue15kto20k;
        outString += "|houseValue20kto25k   : " + houseValue20kto25k;
        outString += "|houseValue25kto30k   : " + houseValue25kto30k;
        outString += "|houseValue30kto35k   : " + houseValue30kto35k;
        outString += "|houseValue35kto40k   : " + houseValue35kto40k;
        outString += "|houseValue40kto45k   : " + houseValue40kto45k;
        outString += "|houseValue45kto50k   : " + houseValue45kto50k;
        outString += "|houseValue50kto60k   : " + houseValue50kto60k;
        outString += "|houseValue60kto75k   : " + houseValue60kto75k;
        outString += "|houseValue75kto100k  : " + houseValue75kto100k;
        outString += "|houseValue100kto125k : " + houseValue100kto125k;
        outString += "|houseValue125kto150k : " + houseValue125kto150k;
        outString += "|houseValue150kto175k : " + houseValue150kto175k;
        outString += "|houseValue175kto200k : " + houseValue175kto200k;
        outString += "|houseValue200kto250k : " + houseValue200kto250k;
        outString += "|houseValue250kto300k : " + houseValue250kto300k;
        outString += "|houseValue300kto400k : " + houseValue300kto400k;
        outString += "|houseValue400kto500k : " + houseValue400kto500k;
        outString += "|houseValue500kAndOver: " + houseValue500kAndOver;
        return outString;
    }
}
