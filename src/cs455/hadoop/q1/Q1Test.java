package cs455.hadoop.q1;

import cs455.hadoop.util.CensusData;

import java.io.*;

/**
 * Created by david on 3/20/17.
 */
public class Q1Test
{
    public static void main(String[] args)
    {

        File file = new File("/Users/david/Downloads/STF1AxAK-F01");
        BufferedReader br = null;
        try
        {
            br = new BufferedReader(new FileReader(file));
        }
        catch (FileNotFoundException e)
        {
            e.printStackTrace();
        }
        String line = null;
        int count = 0;
        try
        {
            while ((line = br.readLine()) != null && count < 100)
            {
                CensusData data = new CensusData(line);
                if (data.getSummaryLevel() == 100)
                {
                    System.out.println("State:" + data.getStateAbbreviation() + " Female Pop:" + data.getFemale() + " Male Pop:" + data.getMale());
                    count++;
                }
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }
}
