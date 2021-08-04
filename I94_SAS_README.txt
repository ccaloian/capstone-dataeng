Column type and descriptions for the raw I94 Immigration Dataset.

For a full mapping between the codes and actual values see `I94_SAS_Labels_Descriptions.sas` file.


CICID - (Float) CIC Id

I94YR - (Float) 4 digit year

I94MON - (Float) Numeric month

I94CIT & I96RES - (Float) This format shows all the valid and invalid codes for processing. 
    See example codes and corresponding value below.
        582 =  'MEXICO Air Sea, and Not Reported (I-94, no land arrivals)'
        101 =  'ALBANIA'
        153 =  'BELARUS'
        310 =  'CAMEROON'
        584 =  'CUBA'
        108 =  'DENMARK'
        506 =  'INVALID: U.S. VIRGIN ISLANDS'
        755 =  'INVALID: WAKE ISLAND'  
        311 =  'Collapsed Tanzania (should not show)'
        741 =  'Collapsed Curacao (should not show)'
         54 =  'No Country Code (54)'
        100 =  'No Country Code (100)'

I94PORT - (String) This format shows all the valid and invalid codes for processing. 
    See example codes and corresponding value below.
        'ALC' = 'ALCAN, AK             '
        'BAR' = 'BAKER AAF - BAKER ISLAND, AK'
        'LIA' = 'LITTLE ROCK, AR (BPS)'
        'ASE' = 'ASPEN, CO #ARPT'
        'COS' = 'COLORADO SPRINGS, CO'
        'CID' = 'CEDAR RAPIDS/IOWA CITY, IA'
        'WSB' = 'WARROAD INTL, SPB, MN'
        'WAR' = 'WARROAD, MN           '
        'DER' = 'DERBY LINE, VT (I-91) '
        'DLV' = 'DERBY LINE, VT (RT. 5)'
        'BLI' = 'BELLINGHAM, WASHINGTON #INTL'
        'BLA' = 'BLAINE, WA            '
        'XXX' = 'NOT REPORTED/UNKNOWN  ' 
        '888' = 'UNIDENTIFED AIR / SEAPORT'
        'UNK' = 'UNKNOWN POE           '
        'HAL' = 'Halifax, NS, Canada   '
        'TOR' = 'TORONTO, CANADA       '
        'AMS' = 'AMSTERDAM-SCHIPHOL, NETHERLANDS'
        'ARB' = 'ARUBA, NETH ANTILLES  '
        'BOG' = 'BOGOTA, EL DORADO #ARPT, COLOMBIA'
        'NCA' = 'NORTH CAICOS, TURK & CAIMAN'
        'GRU' = 'GUARULHOS INTL, SAO PAULO, BRAZIL'
        'ZZZ' = 'MEXICO Land (Banco de Mexico) '
        'CHN' = 'No PORT Code (CHN)'
        'FRG' = 'Collapsed (FOK) 06/15'
        'PHF' = 'No PORT Code (PHF)'

ARRDATE - (Float) Arrival Date in the USA. It is a SAS date numeric field that a 
   permament format has not been applied.  Please apply whichever date format 
   works for you.


I94MODE - (Float) There are missing values as well as not reported (9)
    See example codes and corresponding value below.
        1 = 'Air'
        2 = 'Sea'
        3 = 'Land'
        9 = 'Not reported' ;

I94ADDR - (String) Address in US.
    There is lots of invalid codes in this variable and the list below 
    shows what we have found to be valid, everything else goes into 'other'
    See example codes and corresponding value below.
        'AL'='ALABAMA'
        'DC'='DIST. OF COLUMBIA'
        'VT'='VERMONT'
        'VI'='VIRGIN ISLANDS'
        'VA'='VIRGINIA'
        'WY'='WYOMING' 
        '99'='All Other Codes' ;

DEPDATE - (Float) Departure Date from the USA. It is a SAS date numeric field that 
   a permament format has not been applied.  Please apply whichever date format 
   works for you.


I94BIR - (Float) Age of Respondent in Years


I94VISA - (Float) Visa codes collapsed into three categories:
    See example codes and corresponding value below.
        1 = Business
        2 = Pleasure
        3 = Student

COUNT - (Float) Used for summary statistics.


DTADFILE - (String) Character Date Field - Date added to I-94 Files (yyyyMMdd)


VISAPOST - (String) Department of State where where Visa was issued.


OCCUP - (String) Occupation that will be performed in U.S.


ENTDEPA - (String) Arrival Flag - admitted or paroled into the U.S.


ENTDEPD - (String) Departure Flag - Departed, lost I-94 or is deceased.


ENTDEPU - (String) Update Flag - Either apprehended, overstayed, adjusted to perm residence.


MATFLAG - (String) Match flag - Match of arrival and departure records.


BIRYEAR - (Float) 4 digit year of birth.


DTADDTO - (String) Character Date Field - Date to which admitted to U.S. 
    i.e. allowed to stay until (MMddyyyy).


GENDER - (String) Non-immigrant sex.


INSNUM - (String) INS number.


AIRLINE - (String) Airline used to arrive in U.S.


ADMNUM - (Float) Admission Number.


FLTNO - (String) Flight number of Airline used to arrive in U.S.


VISATYPE - (String) Class of admission legally admitting the non-immigrant to temporarily stay in U.S.

