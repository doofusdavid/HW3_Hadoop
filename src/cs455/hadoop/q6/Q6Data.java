package cs455.hadoop.q6;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class Q6Data implements Writable
{
    @Override
    public String toString()
    {
        return super.toString();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException
    {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException
    {

    }

    public void merge(Q6Data data)
    {
    }
}
