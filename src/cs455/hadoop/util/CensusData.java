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

    // Tenure
    private final int ownerOccupied;
    private final int renterOccupied;

    // Housing: Urban vs Rural

    private final int urbanInsideUrbanizedArea;
    private final int urbanOutsideUrbanizedArea;
    private final int rural;
    private final int urbanRuralNotDefined;

    // Housing: rooms
    private final int house1Room;
    private final int house2Room;
    private final int house3Room;
    private final int house4Room;
    private final int house5Room;
    private final int house6Room;
    private final int house7Room;
    private final int house8Room;
    private final int house9Room;

    // Housing: value owner-occupied
    private final int houseValueUnder15k;
    private final int houseValue15kto20k;
    private final int houseValue20kto25k;
    private final int houseValue25kto30k;
    private final int houseValue30kto35k;
    private final int houseValue35kto40k;
    private final int houseValue40kto45k;
    private final int houseValue45kto50k;
    private final int houseValue50kto60k;
    private final int houseValue60kto75k;
    private final int houseValue75kto100k;
    private final int houseValue100kto125k;
    private final int houseValue125kto150k;
    private final int houseValue150kto175k;
    private final int houseValue175kto200k;
    private final int houseValue200kto250k;
    private final int houseValue250kto300k;
    private final int houseValue300kto400k;
    private final int houseValue400kto500k;
    private final int houseValue500kAndOver;

    // Housing: rent
    private final int rentUnder100;
    private final int rent100To149;
    private final int rent150To199;
    private final int rent200To249;
    private final int rent250To299;
    private final int rent300To349;
    private final int rent350To399;
    private final int rent400To449;
    private final int rent450To499;
    private final int rent500To549;
    private final int rent550To599;
    private final int rent600To649;
    private final int rent650To699;
    private final int rent700To749;
    private final int rent750To999;
    private final int rentOver1000;
    private final int rentNoCashRent;

    public CensusData(String censusDataLine)
    {

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

        // Tenure
        ownerOccupied = Integer.parseInt(eSubstring(censusDataLine, 1804, 9));
        renterOccupied = Integer.parseInt(eSubstring(censusDataLine, 1813, 9));

        // Houses: Urban vs Rural
        urbanInsideUrbanizedArea = Integer.parseInt(eSubstring(censusDataLine, 1822, 9));
        urbanOutsideUrbanizedArea = Integer.parseInt(eSubstring(censusDataLine, 1831, 9));
        rural = Integer.parseInt(eSubstring(censusDataLine, 1840, 9));
        urbanRuralNotDefined = Integer.parseInt(eSubstring(censusDataLine, 1849, 9));

        // Housing: rooms
        house1Room = Integer.parseInt(eSubstring(censusDataLine, 2389, 9));
        house2Room = Integer.parseInt(eSubstring(censusDataLine, 2398, 9));
        house3Room = Integer.parseInt(eSubstring(censusDataLine, 2407, 9));
        house4Room = Integer.parseInt(eSubstring(censusDataLine, 2416, 9));
        house5Room = Integer.parseInt(eSubstring(censusDataLine, 2425, 9));
        house6Room = Integer.parseInt(eSubstring(censusDataLine, 2434, 9));
        house7Room = Integer.parseInt(eSubstring(censusDataLine, 2443, 9));
        house8Room = Integer.parseInt(eSubstring(censusDataLine, 2452, 9));
        house9Room = Integer.parseInt(eSubstring(censusDataLine, 2461, 9));

        // Housing: value owner-occupied
        houseValueUnder15k = Integer.parseInt(eSubstring(censusDataLine, 2929, 9));
        houseValue15kto20k = Integer.parseInt(eSubstring(censusDataLine, 2938, 9));
        houseValue20kto25k = Integer.parseInt(eSubstring(censusDataLine, 2947, 9));
        houseValue25kto30k = Integer.parseInt(eSubstring(censusDataLine, 2956, 9));
        houseValue30kto35k = Integer.parseInt(eSubstring(censusDataLine, 2965, 9));
        houseValue35kto40k = Integer.parseInt(eSubstring(censusDataLine, 2974, 9));
        houseValue40kto45k = Integer.parseInt(eSubstring(censusDataLine, 2983, 9));
        houseValue45kto50k = Integer.parseInt(eSubstring(censusDataLine, 2992, 9));
        houseValue50kto60k = Integer.parseInt(eSubstring(censusDataLine, 3001, 9));
        houseValue60kto75k = Integer.parseInt(eSubstring(censusDataLine, 3010, 9));
        houseValue75kto100k = Integer.parseInt(eSubstring(censusDataLine, 3019, 9));
        houseValue100kto125k = Integer.parseInt(eSubstring(censusDataLine, 3028, 9));
        houseValue125kto150k = Integer.parseInt(eSubstring(censusDataLine, 3037, 9));
        houseValue150kto175k = Integer.parseInt(eSubstring(censusDataLine, 3046, 9));
        houseValue175kto200k = Integer.parseInt(eSubstring(censusDataLine, 3055, 9));
        houseValue200kto250k = Integer.parseInt(eSubstring(censusDataLine, 3064, 9));
        houseValue250kto300k = Integer.parseInt(eSubstring(censusDataLine, 3073, 9));
        houseValue300kto400k = Integer.parseInt(eSubstring(censusDataLine, 3082, 9));
        houseValue400kto500k = Integer.parseInt(eSubstring(censusDataLine, 3091, 9));
        houseValue500kAndOver = Integer.parseInt(eSubstring(censusDataLine, 3100, 9));

        // Housing: rent
        rentUnder100 = Integer.parseInt(eSubstring(censusDataLine, 3451, 9));
        rent100To149 = Integer.parseInt(eSubstring(censusDataLine, 3460, 9));
        rent150To199 = Integer.parseInt(eSubstring(censusDataLine, 3469, 9));
        rent200To249 = Integer.parseInt(eSubstring(censusDataLine, 3478, 9));
        rent250To299 = Integer.parseInt(eSubstring(censusDataLine, 3487, 9));
        rent300To349 = Integer.parseInt(eSubstring(censusDataLine, 3496, 9));
        rent350To399 = Integer.parseInt(eSubstring(censusDataLine, 3505, 9));
        rent400To449 = Integer.parseInt(eSubstring(censusDataLine, 3514, 9));
        rent450To499 = Integer.parseInt(eSubstring(censusDataLine, 3523, 9));
        rent500To549 = Integer.parseInt(eSubstring(censusDataLine, 3532, 9));
        rent550To599 = Integer.parseInt(eSubstring(censusDataLine, 3541, 9));
        rent600To649 = Integer.parseInt(eSubstring(censusDataLine, 3550, 9));
        rent650To699 = Integer.parseInt(eSubstring(censusDataLine, 3559, 9));
        rent700To749 = Integer.parseInt(eSubstring(censusDataLine, 3568, 9));
        rent750To999 = Integer.parseInt(eSubstring(censusDataLine, 3577, 9));
        rentOver1000 = Integer.parseInt(eSubstring(censusDataLine, 3586, 9));
        rentNoCashRent = Integer.parseInt(eSubstring(censusDataLine, 3595, 9));
    }

    /**
     * English friendly substring using offset
     *
     * @param string value to return the substring from
     * @param index  starting index
     * @param offset number of characters
     * @return substring of string, starting at index, for offset characters
     */
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

    public int getAgeUnder1()
    {
        return ageUnder1;
    }

    public int getAgeBetween1and2()
    {
        return ageBetween1and2;
    }

    public int getAgeBetween3and4()
    {
        return ageBetween3and4;
    }

    public int getAge5()
    {
        return age5;
    }

    public int getAge6()
    {
        return age6;
    }

    public int getAgeBetween7and9()
    {
        return ageBetween7and9;
    }

    public int getAgeBetween10and11()
    {
        return ageBetween10and11;
    }

    public int getAgeBetween12and13()
    {
        return ageBetween12and13;
    }

    public int getAge14()
    {
        return age14;
    }

    public int getAge15()
    {
        return age15;
    }

    public int getAge16()
    {
        return age16;
    }

    public int getAge17()
    {
        return age17;
    }

    public int getAge18()
    {
        return age18;
    }

    public int getAge19()
    {
        return age19;
    }

    public int getAge20()
    {
        return age20;
    }

    public int getAge21()
    {
        return age21;
    }

    public int getAgeBetween22and24()
    {
        return ageBetween22and24;
    }

    public int getAgeBetween25and29()
    {
        return ageBetween25and29;
    }

    public int getAgeBetween30and34()
    {
        return ageBetween30and34;
    }

    public int getAgeBetween35and39()
    {
        return ageBetween35and39;
    }

    public int getAgeBetween40and44()
    {
        return ageBetween40and44;
    }

    public int getAgeBetween45and49()
    {
        return ageBetween45and49;
    }

    public int getAgeBetween50and54()
    {
        return ageBetween50and54;
    }

    public int getAgeBetween55and59()
    {
        return ageBetween55and59;
    }

    public int getAgeBetween60and61()
    {
        return ageBetween60and61;
    }

    public int getAgeBetween62and64()
    {
        return ageBetween62and64;
    }

    public int getAgeBetween65and69()
    {
        return ageBetween65and69;
    }

    public int getAgeBetween70and74()
    {
        return ageBetween70and74;
    }

    public int getAgeBetween75and79()
    {
        return ageBetween75and79;
    }

    public int getAgeBetween80and84()
    {
        return ageBetween80and84;
    }

    public int getAge85andOver()
    {
        return age85andOver;
    }

    public int getHispanicMaleAgeUnder1()
    {
        return hispanicMaleAgeUnder1;
    }

    public int getHispanicMaleAgeBetween1and2()
    {
        return hispanicMaleAgeBetween1and2;
    }

    public int getHispanicMaleAgeBetween3and4()
    {
        return hispanicMaleAgeBetween3and4;
    }

    public int getHispanicMaleAge5()
    {
        return hispanicMaleAge5;
    }

    public int getHispanicMaleAge6()
    {
        return hispanicMaleAge6;
    }

    public int getHispanicMaleAgeBetween7and9()
    {
        return hispanicMaleAgeBetween7and9;
    }

    public int getHispanicMaleAgeBetween10and11()
    {
        return hispanicMaleAgeBetween10and11;
    }

    public int getHispanicMaleAgeBetween12and13()
    {
        return hispanicMaleAgeBetween12and13;
    }

    public int getHispanicMaleAge14()
    {
        return hispanicMaleAge14;
    }

    public int getHispanicMaleAge15()
    {
        return hispanicMaleAge15;
    }

    public int getHispanicMaleAge16()
    {
        return hispanicMaleAge16;
    }

    public int getHispanicMaleAge17()
    {
        return hispanicMaleAge17;
    }

    public int getHispanicMaleAge18()
    {
        return hispanicMaleAge18;
    }

    public int getHispanicMaleAge19()
    {
        return hispanicMaleAge19;
    }

    public int getHispanicMaleAge20()
    {
        return hispanicMaleAge20;
    }

    public int getHispanicMaleAge21()
    {
        return hispanicMaleAge21;
    }

    public int getHispanicMaleAgeBetween22and24()
    {
        return hispanicMaleAgeBetween22and24;
    }

    public int getHispanicMaleAgeBetween25and29()
    {
        return hispanicMaleAgeBetween25and29;
    }

    public int getHispanicMaleAgeBetween30and34()
    {
        return hispanicMaleAgeBetween30and34;
    }

    public int getHispanicMaleAgeBetween35and39()
    {
        return hispanicMaleAgeBetween35and39;
    }

    public int getHispanicMaleAgeBetween40and44()
    {
        return hispanicMaleAgeBetween40and44;
    }

    public int getHispanicMaleAgeBetween45and49()
    {
        return hispanicMaleAgeBetween45and49;
    }

    public int getHispanicMaleAgeBetween50and54()
    {
        return hispanicMaleAgeBetween50and54;
    }

    public int getHispanicMaleAgeBetween55and59()
    {
        return hispanicMaleAgeBetween55and59;
    }

    public int getHispanicMaleAgeBetween60and61()
    {
        return hispanicMaleAgeBetween60and61;
    }

    public int getHispanicMaleAgeBetween62and64()
    {
        return hispanicMaleAgeBetween62and64;
    }

    public int getHispanicMaleAgeBetween65and69()
    {
        return hispanicMaleAgeBetween65and69;
    }

    public int getHispanicMaleAgeBetween70and74()
    {
        return hispanicMaleAgeBetween70and74;
    }

    public int getHispanicMaleAgeBetween75and79()
    {
        return hispanicMaleAgeBetween75and79;
    }

    public int getHispanicMaleAgeBetween80and84()
    {
        return hispanicMaleAgeBetween80and84;
    }

    public int getHispanicMaleAge85andOver()
    {
        return hispanicMaleAge85andOver;
    }

    public int getHispanicFemaleAgeUnder1()
    {
        return hispanicFemaleAgeUnder1;
    }

    public int getHispanicFemaleAgeBetween1and2()
    {
        return hispanicFemaleAgeBetween1and2;
    }

    public int getHispanicFemaleAgeBetween3and4()
    {
        return hispanicFemaleAgeBetween3and4;
    }

    public int getHispanicFemaleAge5()
    {
        return hispanicFemaleAge5;
    }

    public int getHispanicFemaleAge6()
    {
        return hispanicFemaleAge6;
    }

    public int getHispanicFemaleAgeBetween7and9()
    {
        return hispanicFemaleAgeBetween7and9;
    }

    public int getHispanicFemaleAgeBetween10and11()
    {
        return hispanicFemaleAgeBetween10and11;
    }

    public int getHispanicFemaleAgeBetween12and13()
    {
        return hispanicFemaleAgeBetween12and13;
    }

    public int getHispanicFemaleAge14()
    {
        return hispanicFemaleAge14;
    }

    public int getHispanicFemaleAge15()
    {
        return hispanicFemaleAge15;
    }

    public int getHispanicFemaleAge16()
    {
        return hispanicFemaleAge16;
    }

    public int getHispanicFemaleAge17()
    {
        return hispanicFemaleAge17;
    }

    public int getHispanicFemaleAge18()
    {
        return hispanicFemaleAge18;
    }

    public int getHispanicFemaleAge19()
    {
        return hispanicFemaleAge19;
    }

    public int getHispanicFemaleAge20()
    {
        return hispanicFemaleAge20;
    }

    public int getHispanicFemaleAge21()
    {
        return hispanicFemaleAge21;
    }

    public int getHispanicFemaleAgeBetween22and24()
    {
        return hispanicFemaleAgeBetween22and24;
    }

    public int getHispanicFemaleAgeBetween25and29()
    {
        return hispanicFemaleAgeBetween25and29;
    }

    public int getHispanicFemaleAgeBetween30and34()
    {
        return hispanicFemaleAgeBetween30and34;
    }

    public int getHispanicFemaleAgeBetween35and39()
    {
        return hispanicFemaleAgeBetween35and39;
    }

    public int getHispanicFemaleAgeBetween40and44()
    {
        return hispanicFemaleAgeBetween40and44;
    }

    public int getHispanicFemaleAgeBetween45and49()
    {
        return hispanicFemaleAgeBetween45and49;
    }

    public int getHispanicFemaleAgeBetween50and54()
    {
        return hispanicFemaleAgeBetween50and54;
    }

    public int getHispanicFemaleAgeBetween55and59()
    {
        return hispanicFemaleAgeBetween55and59;
    }

    public int getHispanicFemaleAgeBetween60and61()
    {
        return hispanicFemaleAgeBetween60and61;
    }

    public int getHispanicFemaleAgeBetween62and64()
    {
        return hispanicFemaleAgeBetween62and64;
    }

    public int getHispanicFemaleAgeBetween65and69()
    {
        return hispanicFemaleAgeBetween65and69;
    }

    public int getHispanicFemaleAgeBetween70and74()
    {
        return hispanicFemaleAgeBetween70and74;
    }

    public int getHispanicFemaleAgeBetween75and79()
    {
        return hispanicFemaleAgeBetween75and79;
    }

    public int getHispanicFemaleAgeBetween80and84()
    {
        return hispanicFemaleAgeBetween80and84;
    }

    public int getHispanicFemaleAge85andOver()
    {
        return hispanicFemaleAge85andOver;
    }

    public int getMaleNeverMarried()
    {
        return maleNeverMarried;
    }

    public int getMaleMarriedButSeparated()
    {
        return maleMarriedButSeparated;
    }

    public int getMaleSeparated()
    {
        return maleSeparated;
    }

    public int getMaleWidowed()
    {
        return maleWidowed;
    }

    public int getFemaleNeverMarried()
    {
        return femaleNeverMarried;
    }

    public int getFemaleMarriedButSeparated()
    {
        return femaleMarriedButSeparated;
    }

    public int getFemaleSeparated()
    {
        return femaleSeparated;
    }

    public int getFemaleWidowed()
    {
        return femaleWidowed;
    }

    public int getOwnerOccupied()
    {
        return ownerOccupied;
    }

    public int getRenterOccupied()
    {
        return renterOccupied;
    }

    public int getUrbanInsideUrbanizedArea()
    {
        return urbanInsideUrbanizedArea;
    }

    public int getUrbanOutsideUrbanizedArea()
    {
        return urbanOutsideUrbanizedArea;
    }

    public int getRural()
    {
        return rural;
    }

    public int getUrbanRuralNotDefined()
    {
        return urbanRuralNotDefined;
    }

    public int getHouse1Room()
    {
        return house1Room;
    }

    public int getHouse2Room()
    {
        return house2Room;
    }

    public int getHouse3Room()
    {
        return house3Room;
    }

    public int getHouse4Room()
    {
        return house4Room;
    }

    public int getHouse5Room()
    {
        return house5Room;
    }

    public int getHouse6Room()
    {
        return house6Room;
    }

    public int getHouse7Room()
    {
        return house7Room;
    }

    public int getHouse8Room()
    {
        return house8Room;
    }

    public int getHouse9Room()
    {
        return house9Room;
    }

    public int getHouseValueUnder15k()
    {
        return houseValueUnder15k;
    }

    public int getHouseValue15kto20k()
    {
        return houseValue15kto20k;
    }

    public int getHouseValue20kto25k()
    {
        return houseValue20kto25k;
    }

    public int getHouseValue25kto30k()
    {
        return houseValue25kto30k;
    }

    public int getHouseValue30kto35k()
    {
        return houseValue30kto35k;
    }

    public int getHouseValue35kto40k()
    {
        return houseValue35kto40k;
    }

    public int getHouseValue40kto45k()
    {
        return houseValue40kto45k;
    }

    public int getHouseValue45kto50k()
    {
        return houseValue45kto50k;
    }

    public int getHouseValue50kto60k()
    {
        return houseValue50kto60k;
    }

    public int getHouseValue60kto75k()
    {
        return houseValue60kto75k;
    }

    public int getHouseValue75kto100k()
    {
        return houseValue75kto100k;
    }

    public int getHouseValue100kto125k()
    {
        return houseValue100kto125k;
    }

    public int getHouseValue125kto150k()
    {
        return houseValue125kto150k;
    }

    public int getHouseValue150kto175k()
    {
        return houseValue150kto175k;
    }

    public int getHouseValue175kto200k()
    {
        return houseValue175kto200k;
    }

    public int getHouseValue200kto250k()
    {
        return houseValue200kto250k;
    }

    public int getHouseValue250kto300k()
    {
        return houseValue250kto300k;
    }

    public int getHouseValue300kto400k()
    {
        return houseValue300kto400k;
    }

    public int getHouseValue400kto500k()
    {
        return houseValue400kto500k;
    }

    public int getHouseValue500kAndOver()
    {
        return houseValue500kAndOver;
    }

    public int getRentUnder100()
    {
        return rentUnder100;
    }

    public int getRent100To149()
    {
        return rent100To149;
    }

    public int getRent150To199()
    {
        return rent150To199;
    }

    public int getRent200To249()
    {
        return rent200To249;
    }

    public int getRent250To299()
    {
        return rent250To299;
    }

    public int getRent300To349()
    {
        return rent300To349;
    }

    public int getRent350To399()
    {
        return rent350To399;
    }

    public int getRent400To449()
    {
        return rent400To449;
    }

    public int getRent450To499()
    {
        return rent450To499;
    }

    public int getRent500To549()
    {
        return rent500To549;
    }

    public int getRent550To599()
    {
        return rent550To599;
    }

    public int getRent600To649()
    {
        return rent600To649;
    }

    public int getRent650To699()
    {
        return rent650To699;
    }

    public int getRent700To749()
    {
        return rent700To749;
    }

    public int getRent750To999()
    {
        return rent750To999;
    }

    public int getRentOver1000()
    {
        return rentOver1000;
    }

    public int getRentNoCashRent()
    {
        return rentNoCashRent;
    }

}
