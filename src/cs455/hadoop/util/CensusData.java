package cs455.hadoop.util;

public class CensusData
{
    // Administrative Items
    private final String stateAbbreviation;
    private final int summaryLevel;
    private final int logicalRecordNumber;
    private final int logicalRecordPartNumber;
    private final int totalNumberOfPartsInRecord;
    // Population
    private final int persons;
    // Urban and Rural
    private final int insideUrbanizedArea;
    private final int outsideUrbanizedArea;
    // Gender
    private final int male;
    private final int female;

    // Age
    private final int ageUnder1;
    private final int ageBetween1and2;
    private final int ageBetween3and4;
    private final int age5;
    private final int age6;
    private final int ageBetween7and9;
    private final int ageBetween10and11;
    private final int ageBetween12and13;
    private final int age14;
    private final int age15;
    private final int age16;
    private final int age17;
    private final int age18;
    private final int age19;
    private final int age20;
    private final int age21;
    private final int ageBetween22and24;
    private final int ageBetween25and29;
    private final int ageBetween30and34;
    private final int ageBetween35and39;
    private final int ageBetween40and44;
    private final int ageBetween45and49;
    private final int ageBetween50and54;
    private final int ageBetween55and59;
    private final int ageBetween60and61;
    private final int ageBetween62and64;
    private final int ageBetween65and69;
    private final int ageBetween70and74;
    private final int ageBetween75and79;
    private final int ageBetween80and84;
    private final int age85andOver;

    // Hispanic Male
    private final int hispanicMaleAgeUnder1;
    private final int hispanicMaleAgeBetween1and2;
    private final int hispanicMaleAgeBetween3and4;
    private final int hispanicMaleAge5;
    private final int hispanicMaleAge6;
    private final int hispanicMaleAgeBetween7and9;
    private final int hispanicMaleAgeBetween10and11;
    private final int hispanicMaleAgeBetween12and13;
    private final int hispanicMaleAge14;
    private final int hispanicMaleAge15;
    private final int hispanicMaleAge16;
    private final int hispanicMaleAge17;
    private final int hispanicMaleAge18;
    private final int hispanicMaleAge19;
    private final int hispanicMaleAge20;
    private final int hispanicMaleAge21;
    private final int hispanicMaleAgeBetween22and24;
    private final int hispanicMaleAgeBetween25and29;
    private final int hispanicMaleAgeBetween30and34;
    private final int hispanicMaleAgeBetween35and39;
    private final int hispanicMaleAgeBetween40and44;
    private final int hispanicMaleAgeBetween45and49;
    private final int hispanicMaleAgeBetween50and54;
    private final int hispanicMaleAgeBetween55and59;
    private final int hispanicMaleAgeBetween60and61;
    private final int hispanicMaleAgeBetween62and64;
    private final int hispanicMaleAgeBetween65and69;
    private final int hispanicMaleAgeBetween70and74;
    private final int hispanicMaleAgeBetween75and79;
    private final int hispanicMaleAgeBetween80and84;
    private final int hispanicMaleAge85andOver;

    // Hispanic Female
    private final int hispanicFemaleAgeUnder1;
    private final int hispanicFemaleAgeBetween1and2;
    private final int hispanicFemaleAgeBetween3and4;
    private final int hispanicFemaleAge5;
    private final int hispanicFemaleAge6;
    private final int hispanicFemaleAgeBetween7and9;
    private final int hispanicFemaleAgeBetween10and11;
    private final int hispanicFemaleAgeBetween12and13;
    private final int hispanicFemaleAge14;
    private final int hispanicFemaleAge15;
    private final int hispanicFemaleAge16;
    private final int hispanicFemaleAge17;
    private final int hispanicFemaleAge18;
    private final int hispanicFemaleAge19;
    private final int hispanicFemaleAge20;
    private final int hispanicFemaleAge21;
    private final int hispanicFemaleAgeBetween22and24;
    private final int hispanicFemaleAgeBetween25and29;
    private final int hispanicFemaleAgeBetween30and34;
    private final int hispanicFemaleAgeBetween35and39;
    private final int hispanicFemaleAgeBetween40and44;
    private final int hispanicFemaleAgeBetween45and49;
    private final int hispanicFemaleAgeBetween50and54;
    private final int hispanicFemaleAgeBetween55and59;
    private final int hispanicFemaleAgeBetween60and61;
    private final int hispanicFemaleAgeBetween62and64;
    private final int hispanicFemaleAgeBetween65and69;
    private final int hispanicFemaleAgeBetween70and74;
    private final int hispanicFemaleAgeBetween75and79;
    private final int hispanicFemaleAgeBetween80and84;
    private final int hispanicFemaleAge85andOver;

    // Male Marriage Status
    private final int maleNeverMarried;
    private final int maleMarriedButSeparated;
    private final int maleSeparated;
    private final int maleWidowed;

    // Female Marriage Status
    private final int femaleNeverMarried;
    private final int femaleMarriedButSeparated;
    private final int femaleSeparated;
    private final int femaleWidowed;



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

        // Age
        ageUnder1 = Integer.parseInt(eSubstring(censusDataLine, 796, 9));
        ageBetween1and2 = Integer.parseInt(eSubstring(censusDataLine, 805, 9));
        ageBetween3and4 = Integer.parseInt(eSubstring(censusDataLine, 814, 9));
        age5 = Integer.parseInt(eSubstring(censusDataLine, 823, 9));
        age6 = Integer.parseInt(eSubstring(censusDataLine, 832, 9));
        ageBetween7and9 = Integer.parseInt(eSubstring(censusDataLine, 841, 9));
        ageBetween10and11 = Integer.parseInt(eSubstring(censusDataLine, 850, 9));
        ageBetween12and13 = Integer.parseInt(eSubstring(censusDataLine, 859, 9));
        age14 = Integer.parseInt(eSubstring(censusDataLine, 868, 9));
        age15 = Integer.parseInt(eSubstring(censusDataLine, 877, 9));
        age16 = Integer.parseInt(eSubstring(censusDataLine, 886, 9));
        age17 = Integer.parseInt(eSubstring(censusDataLine, 895, 9));
        age18 = Integer.parseInt(eSubstring(censusDataLine, 904, 9));
        age19 = Integer.parseInt(eSubstring(censusDataLine, 913, 9));
        age20 = Integer.parseInt(eSubstring(censusDataLine, 922, 9));
        age21 = Integer.parseInt(eSubstring(censusDataLine, 931, 9));
        ageBetween22and24 = Integer.parseInt(eSubstring(censusDataLine, 940, 9));
        ageBetween25and29 = Integer.parseInt(eSubstring(censusDataLine, 949, 9));
        ageBetween30and34 = Integer.parseInt(eSubstring(censusDataLine, 958, 9));
        ageBetween35and39 = Integer.parseInt(eSubstring(censusDataLine, 967, 9));
        ageBetween40and44 = Integer.parseInt(eSubstring(censusDataLine, 976, 9));
        ageBetween45and49 = Integer.parseInt(eSubstring(censusDataLine, 985, 9));
        ageBetween50and54 = Integer.parseInt(eSubstring(censusDataLine, 994, 9));
        ageBetween55and59 = Integer.parseInt(eSubstring(censusDataLine, 1003, 9));
        ageBetween60and61 = Integer.parseInt(eSubstring(censusDataLine, 1012, 9));
        ageBetween62and64 = Integer.parseInt(eSubstring(censusDataLine, 1021, 9));
        ageBetween65and69 = Integer.parseInt(eSubstring(censusDataLine, 1030, 9));
        ageBetween70and74 = Integer.parseInt(eSubstring(censusDataLine, 1039, 9));
        ageBetween75and79 = Integer.parseInt(eSubstring(censusDataLine, 1048, 9));
        ageBetween80and84 = Integer.parseInt(eSubstring(censusDataLine, 1057, 9));
        age85andOver = Integer.parseInt(eSubstring(censusDataLine, 1066, 9));

        // Hispanic Male
        hispanicMaleAgeUnder1 = Integer.parseInt(eSubstring(censusDataLine, 3865, 9));
        hispanicMaleAgeBetween1and2 = Integer.parseInt(eSubstring(censusDataLine, 3874, 9));
        hispanicMaleAgeBetween3and4 = Integer.parseInt(eSubstring(censusDataLine, 3883, 9));
        hispanicMaleAge5 = Integer.parseInt(eSubstring(censusDataLine, 3892, 9));
        hispanicMaleAge6 = Integer.parseInt(eSubstring(censusDataLine, 3901, 9));
        hispanicMaleAgeBetween7and9 = Integer.parseInt(eSubstring(censusDataLine, 3910, 9));
        hispanicMaleAgeBetween10and11 = Integer.parseInt(eSubstring(censusDataLine, 3919, 9));
        hispanicMaleAgeBetween12and13 = Integer.parseInt(eSubstring(censusDataLine, 3928, 9));
        hispanicMaleAge14 = Integer.parseInt(eSubstring(censusDataLine, 3937, 9));
        hispanicMaleAge15 = Integer.parseInt(eSubstring(censusDataLine, 3946, 9));
        hispanicMaleAge16 = Integer.parseInt(eSubstring(censusDataLine, 3955, 9));
        hispanicMaleAge17 = Integer.parseInt(eSubstring(censusDataLine, 3964, 9));
        hispanicMaleAge18 = Integer.parseInt(eSubstring(censusDataLine, 3973, 9));
        hispanicMaleAge19 = Integer.parseInt(eSubstring(censusDataLine, 3982, 9));
        hispanicMaleAge20 = Integer.parseInt(eSubstring(censusDataLine, 3991, 9));
        hispanicMaleAge21 = Integer.parseInt(eSubstring(censusDataLine, 4000, 9));
        hispanicMaleAgeBetween22and24 = Integer.parseInt(eSubstring(censusDataLine, 4009, 9));
        hispanicMaleAgeBetween25and29 = Integer.parseInt(eSubstring(censusDataLine, 4018, 9));
        hispanicMaleAgeBetween30and34 = Integer.parseInt(eSubstring(censusDataLine, 4027, 9));
        hispanicMaleAgeBetween35and39 = Integer.parseInt(eSubstring(censusDataLine, 4036, 9));
        hispanicMaleAgeBetween40and44 = Integer.parseInt(eSubstring(censusDataLine, 4045, 9));
        hispanicMaleAgeBetween45and49 = Integer.parseInt(eSubstring(censusDataLine, 4054, 9));
        hispanicMaleAgeBetween50and54 = Integer.parseInt(eSubstring(censusDataLine, 4063, 9));
        hispanicMaleAgeBetween55and59 = Integer.parseInt(eSubstring(censusDataLine, 4072, 9));
        hispanicMaleAgeBetween60and61 = Integer.parseInt(eSubstring(censusDataLine, 4081, 9));
        hispanicMaleAgeBetween62and64 = Integer.parseInt(eSubstring(censusDataLine, 4090, 9));
        hispanicMaleAgeBetween65and69 = Integer.parseInt(eSubstring(censusDataLine, 4099, 9));
        hispanicMaleAgeBetween70and74 = Integer.parseInt(eSubstring(censusDataLine, 4108, 9));
        hispanicMaleAgeBetween75and79 = Integer.parseInt(eSubstring(censusDataLine, 4117, 9));
        hispanicMaleAgeBetween80and84 = Integer.parseInt(eSubstring(censusDataLine, 4126, 9));
        hispanicMaleAge85andOver = Integer.parseInt(eSubstring(censusDataLine, 4135, 9));

        // Hispanic Female
        hispanicFemaleAgeUnder1 = Integer.parseInt(eSubstring(censusDataLine, 4144, 9));
        hispanicFemaleAgeBetween1and2 = Integer.parseInt(eSubstring(censusDataLine, 4153, 9));
        hispanicFemaleAgeBetween3and4 = Integer.parseInt(eSubstring(censusDataLine, 4162, 9));
        hispanicFemaleAge5 = Integer.parseInt(eSubstring(censusDataLine, 4171, 9));
        hispanicFemaleAge6 = Integer.parseInt(eSubstring(censusDataLine, 4180, 9));
        hispanicFemaleAgeBetween7and9 = Integer.parseInt(eSubstring(censusDataLine, 4189, 9));
        hispanicFemaleAgeBetween10and11 = Integer.parseInt(eSubstring(censusDataLine, 4198, 9));
        hispanicFemaleAgeBetween12and13 = Integer.parseInt(eSubstring(censusDataLine, 4207, 9));
        hispanicFemaleAge14 = Integer.parseInt(eSubstring(censusDataLine, 4216, 9));
        hispanicFemaleAge15 = Integer.parseInt(eSubstring(censusDataLine, 4225, 9));
        hispanicFemaleAge16 = Integer.parseInt(eSubstring(censusDataLine, 4234, 9));
        hispanicFemaleAge17 = Integer.parseInt(eSubstring(censusDataLine, 4243, 9));
        hispanicFemaleAge18 = Integer.parseInt(eSubstring(censusDataLine, 4252, 9));
        hispanicFemaleAge19 = Integer.parseInt(eSubstring(censusDataLine, 4261, 9));
        hispanicFemaleAge20 = Integer.parseInt(eSubstring(censusDataLine, 4270, 9));
        hispanicFemaleAge21 = Integer.parseInt(eSubstring(censusDataLine, 4279, 9));
        hispanicFemaleAgeBetween22and24 = Integer.parseInt(eSubstring(censusDataLine, 4288, 9));
        hispanicFemaleAgeBetween25and29 = Integer.parseInt(eSubstring(censusDataLine, 4297, 9));
        hispanicFemaleAgeBetween30and34 = Integer.parseInt(eSubstring(censusDataLine, 4306, 9));
        hispanicFemaleAgeBetween35and39 = Integer.parseInt(eSubstring(censusDataLine, 4315, 9));
        hispanicFemaleAgeBetween40and44 = Integer.parseInt(eSubstring(censusDataLine, 4324, 9));
        hispanicFemaleAgeBetween45and49 = Integer.parseInt(eSubstring(censusDataLine, 4333, 9));
        hispanicFemaleAgeBetween50and54 = Integer.parseInt(eSubstring(censusDataLine, 4342, 9));
        hispanicFemaleAgeBetween55and59 = Integer.parseInt(eSubstring(censusDataLine, 4351, 9));
        hispanicFemaleAgeBetween60and61 = Integer.parseInt(eSubstring(censusDataLine, 4360, 9));
        hispanicFemaleAgeBetween62and64 = Integer.parseInt(eSubstring(censusDataLine, 4369, 9));
        hispanicFemaleAgeBetween65and69 = Integer.parseInt(eSubstring(censusDataLine, 4378, 9));
        hispanicFemaleAgeBetween70and74 = Integer.parseInt(eSubstring(censusDataLine, 4387, 9));
        hispanicFemaleAgeBetween75and79 = Integer.parseInt(eSubstring(censusDataLine, 4396, 9));
        hispanicFemaleAgeBetween80and84 = Integer.parseInt(eSubstring(censusDataLine, 4405, 9));
        hispanicFemaleAge85andOver = Integer.parseInt(eSubstring(censusDataLine, 4414, 9));

        // Male Marriage Status
        maleNeverMarried = Integer.parseInt(eSubstring(censusDataLine, 4423, 9));
        maleMarriedButSeparated = Integer.parseInt(eSubstring(censusDataLine, 4432, 9));
        maleSeparated = Integer.parseInt(eSubstring(censusDataLine, 4441, 9));
        maleWidowed = Integer.parseInt(eSubstring(censusDataLine, 4450, 9));

        // Female Marriage Status
        femaleNeverMarried = Integer.parseInt(eSubstring(censusDataLine, 4468, 9));
        femaleMarriedButSeparated = Integer.parseInt(eSubstring(censusDataLine, 4477, 9));
        femaleSeparated = Integer.parseInt(eSubstring(censusDataLine, 4486, 9));
        femaleWidowed = Integer.parseInt(eSubstring(censusDataLine, 4495, 9));

    }

    public static String eSubstring(String string, int index, int offset)
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
