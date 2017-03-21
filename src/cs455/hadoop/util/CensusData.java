package cs455.hadoop.util;

public class CensusData
{
    // Administrative Items
    private String stateAbbreviation;
    private int summaryLevel;
    private int logicalRecordNumber;
    private int logicalRecordPartNumber;
    private int totalNumberOfPartsInRecord;
    // Population
    private int persons;
    // Urban and Rural
    private int insideUrbanizedArea;
    private int outsideUrbanizedArea;
    // Gender
    private int male;
    private int female;

    public CensusData(String censusDataLine)
    {
        if (censusDataLine.length() < 300)
            throw new IllegalArgumentException("Data Line length:" + censusDataLine.length());

        // Administrative Items
        stateAbbreviation = censusDataLine.substring(9, 2);
        summaryLevel = Integer.parseInt(censusDataLine.substring(11, 3));
        logicalRecordNumber = Integer.parseInt(censusDataLine.substring(19, 6));
        logicalRecordPartNumber = Integer.parseInt(censusDataLine.substring(25, 4));
        totalNumberOfPartsInRecord = Integer.parseInt(censusDataLine.substring(29, 4));

        // Population
        persons = Integer.parseInt(censusDataLine.substring(301, 9));

        // Urban and Rural
        insideUrbanizedArea = Integer.parseInt(censusDataLine.substring(328, 9));
        outsideUrbanizedArea = Integer.parseInt(censusDataLine.substring(337, 9));

        // Gender
        male = Integer.parseInt(censusDataLine.substring(364, 9));
        female = Integer.parseInt(censusDataLine.substring(373, 9));
    }

    public String getStateAbbreviation()
    {
        return stateAbbreviation;
    }

    public int getSummaryLevel()
    {
        return summaryLevel;
    }

    public int getLogicalRecordNumber()
    {
        return logicalRecordNumber;
    }

    public int getLogicalRecordPartNumber()
    {
        return logicalRecordPartNumber;
    }

    public int getTotalNumberOfPartsInRecord()
    {
        return totalNumberOfPartsInRecord;
    }

    public int getPersons()
    {
        return persons;
    }

    public int getInsideUrbanizedArea()
    {
        return insideUrbanizedArea;
    }

    public int getOutsideUrbanizedArea()
    {
        return outsideUrbanizedArea;
    }

    public int getMale()
    {
        return male;
    }

    public int getFemale()
    {
        return female;
    }
}
