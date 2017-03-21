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
        stateAbbreviation = eSubstring(censusDataLine, 9, 2);
        summaryLevel = Integer.parseInt(eSubstring(censusDataLine, 11, 3));
        logicalRecordNumber = Integer.parseInt(eSubstring(censusDataLine, 19, 6));
        logicalRecordPartNumber = Integer.parseInt(eSubstring(censusDataLine, 25, 4));
        totalNumberOfPartsInRecord = Integer.parseInt(eSubstring(censusDataLine, 29, 4));

        // Population
        persons = Integer.parseInt(eSubstring(censusDataLine, 301, 9));

        // Urban and Rural
        insideUrbanizedArea = Integer.parseInt(eSubstring(censusDataLine, 328, 9));
        outsideUrbanizedArea = Integer.parseInt(eSubstring(censusDataLine, 337, 9));

        // Gender
        male = Integer.parseInt(eSubstring(censusDataLine, 364, 9));
        female = Integer.parseInt(eSubstring(censusDataLine, 373, 9));
    }

    public String eSubstring(String string, int index, int offset)
    {
        int englishIndex = index - 1;
        int englishEnd = index - 1 + offset;
        return string.substring(englishIndex, englishEnd);
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
