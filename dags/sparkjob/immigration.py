
import os
from datetime import timedelta
from datetime import datetime
from pyspark.sql.functions import col,split,expr,udf,trim,mean,sum,create_map,explode,year
import time
from pyspark.sql import functions as F
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql import types as T
from pyspark.sql.functions import expr
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType,DateType,MapType
import re



paramSchema = StructType([StructField('_c0',DoubleType()),StructField('cicid',DoubleType()),
                          StructField('i94yr',DoubleType()),
                          StructField('i94mon',DoubleType()),StructField('i94cit',DoubleType()),
                          StructField('i94res',DoubleType()),StructField('i94port',StringType()),
                          StructField('arrdate',DoubleType()),StructField('i94mode',DoubleType()),
                          StructField('i94addr',StringType()),StructField('depdate',DoubleType()),
                          StructField('i94bir',DoubleType()),StructField('i94visa',DoubleType()),
                          StructField('count',DoubleType()),StructField('dtadfile',StringType()),
                          StructField('visapost',StringType()),StructField('occup',StringType()),
                          StructField('entdepa',StringType()),StructField('entdepd',StringType()),
                          StructField('entdepu',StringType()),StructField('matflag',StringType()),
                          StructField('biryear',DoubleType()),StructField('dtaddto',StringType()),
                          StructField('gender',StringType()),StructField('insnum',StringType()),
                          StructField('airline',StringType()),StructField('admnum',DoubleType()),
                          StructField('fltno',StringType()),StructField('visatype',StringType())])


country_code ={582 :'MEXICO Air Sea, and Not Reported (I-94, no land arrivals)',236:'AFGHANISTAN',101:'ALBANIA',316:'ALGERIA',
102 :'ANDORRA',324:'ANGOLA',529:'ANGUILLA',518:'ANTIGUA-BARBUDA',687:'ARGENTINA',151:'ARMENIA',532:'ARUBA',
438:'AUSTRALIA',103:'AUSTRIA',152:'AZERBAIJAN',512:'BAHAMAS',298:'BAHRAIN',274:'BANGLADESH',513:'BARBADOS',
104:'BELGIUM',581:'BELIZE',386:'BENIN',509:'BERMUDA',153:'BELARUS',242:'BHUTAN',688:'BOLIVIA',717:'BONAIRE, ST EUSTATIUS, SABA',
164:'BOSNIA-HERZEGOVINA',336:'BOTSWANA',689:'BRAZIL',525:'BRITISH VIRGIN ISLANDS',217:'BRUNEI',105:'BULGARIA',
393:'BURKINA FASO',243:'BURMA',375:'BURUNDI',310:'CAMEROON',326:'CAPE VERDE',526:'CAYMAN ISLANDS',383:'CENTRAL AFRICAN REPUBLIC',
384:'CHAD',690:'CHILE',245:'CHINA, PRC',721:'CURACAO',270:'CHRISTMAS ISLAND',271:'COCOS ISLANDS',691:'COLOMBIA',
317:'COMOROS',385:'CONGO',467:'COOK ISLANDS',575:'COSTA RICA',165:'CROATIA',584:'CUBA',218:'CYPRUS',
140:'CZECH REPUBLIC',723:'FAROE ISLANDS (PART OF DENMARK)',108:'DENMARK',322:'DJIBOUTI',519:'DOMINICA',
585:'DOMINICAN REPUBLIC',240:'EAST TIMOR',692:'ECUADOR',368:'EGYPT',576:'EL SALVADOR',399:'EQUATORIAL GUINEA',
372:'ERITREA',109:'ESTONIA',369:'ETHIOPIA',604:'FALKLAND ISLANDS',413:'FIJI',110:'FINLAND',111:'FRANCE',
601:'FRENCH GUIANA',411:'FRENCH POLYNESIA',387:'GABON',338:'GAMBIA',758:'GAZA STRIP', 154:'GEORGIA',
112:'GERMANY',339:'GHANA',143:'GIBRALTAR',113:'GREECE',520:'GRENADA',507:'GUADELOUPE',577:'GUATEMALA',382:'GUINEA',
327:'GUINEA-BISSAU',603:'GUYANA',586:'HAITI',726:'HEARD AND MCDONALD IS.',149:'HOLY SEE/VATICAN',528:'HONDURAS',
206:'HONG KONG',114:'HUNGARY',115:'ICELAND',213:'INDIA',759:'INDIAN OCEAN AREAS (FRENCH)',729:'INDIAN OCEAN TERRITORY',
204:'INDONESIA',249:'IRAN',250:'IRAQ',116:'IRELAND',251:'ISRAEL',117:'ITALY',388:'IVORY COAST',514:'JAMAICA',
209:'JAPAN',253:'JORDAN',201:'KAMPUCHEA',155:'KAZAKHSTAN',340:'KENYA',414:'KIRIBATI',732:'KOSOVO',272:'KUWAIT',
156:'KYRGYZSTAN',203:'LAOS',118:'LATVIA',255:'LEBANON',335:'LESOTHO',370:'LIBERIA',381:'LIBYA',119:'LIECHTENSTEIN',
120:'LITHUANIA',121:'LUXEMBOURG',214:'MACAU',167:'MACEDONIA',320:'MADAGASCAR',345:'MALAWI',273:'MALAYSIA',
220:'MALDIVES',392:'MALI',145:'MALTA',472:'MARSHALL ISLANDS',511:'MARTINIQUE',389:'MAURITANIA',342:'MAURITIUS',
760:'MAYOTTE (AFRICA - FRENCH)',473:'MICRONESIA, FED. STATES OF',157:'MOLDOVA',122:'MONACO',299:'MONGOLIA',
735:'MONTENEGRO',521:'MONTSERRAT',332:'MOROCCO',329:'MOZAMBIQUE',371:'NAMIBIA',440:'NAURU',257:'NEPAL',
123:'NETHERLANDS',508:'NETHERLANDS ANTILLES',409:'NEW CALEDONIA',464:'NEW ZEALAND',579:'NICARAGUA',390:'NIGER',
343:'NIGERIA',470:'NIUE',275:'NORTH KOREA',124:'NORWAY',256:'OMAN',258:'PAKISTAN',474:'PALAU',743:'PALESTINE',
504:'PANAMA',441:'PAPUA NEW GUINEA',693:'PARAGUAY',694:'PERU',260:'PHILIPPINES',416:'PITCAIRN ISLANDS',107:'POLAND',
126:'PORTUGAL',297:'QATAR',748:'REPUBLIC OF SOUTH SUDAN',321:'REUNION',127:'ROMANIA',158:'RUSSIA',
376:'RWANDA',128:'SAN MARINO',330:'SAO TOME AND PRINCIPE',261:'SAUDI ARABIA',391:'SENEGAL',
142:'SERBIA AND MONTENEGRO',745:'SERBIA',347:'SEYCHELLES',348:'SIERRA LEONE',207:'SINGAPORE',141:'SLOVAKIA',
166:'SLOVENIA',412:'SOLOMON ISLANDS',397:'SOMALIA',373:'SOUTH AFRICA',276:'SOUTH KOREA',129:'SPAIN',
244:'SRI LANKA',346:'ST. HELENA',522:'ST. KITTS-NEVIS',523:'ST. LUCIA',502:'ST. PIERRE AND MIQUELON',
524:'ST. VINCENT-GRENADINES',716:'SAINT BARTHELEMY',736:'SAINT MARTIN',749:'SAINT MAARTEN',350:'SUDAN',
602:'SURINAME',351:'SWAZILAND',130:'SWEDEN',131:'SWITZERLAND',262:'SYRIA',268:'TAIWAN',159:'TAJIKISTAN',
353:'TANZANIA',263:'THAILAND',304:'TOGO',417:'TONGA',516:'TRINIDAD AND TOBAGO',323:'TUNISIA',264:'TURKEY',
161:'TURKMENISTAN',527:'TURKS AND CAICOS ISLANDS',420:'TUVALU',352:'UGANDA',162 :'UKRAINE',
296:'UNITED ARAB EMIRATES',135:'UNITED KINGDOM',695:'URUGUAY',163:'UZBEKISTAN',410:'VANUATU',
696:'VENEZUELA',266:'VIETNAM',469:'WALLIS AND FUTUNA ISLANDS',757:'WEST INDIES (FRENCH)',333:'WESTERN SAHARA',
465:'WESTERN SAMOA',216:'YEMEN',139:'YUGOSLAVIA',301:'ZAIRE',344:'ZAMBIA',315:'ZIMBABWE',
403:'INVALID: AMERICAN SAMOA',712:'INVALID: ANTARCTICA' ,700:'INVALID: BORN ON BOARD SHIP',
719:'INVALID: BOUVET ISLAND (ANTARCTICA/NORWAY TERR.)',574:'INVALID: CANADA',
720:'INVALID: CANTON AND ENDERBURY ISLS',106:'INVALID: CZECHOSLOVAKIA',
739:'INVALID: DRONNING MAUD LAND (ANTARCTICA-NORWAY)',394:'INVALID: FRENCH SOUTHERN AND ANTARCTIC',
501:'INVALID: GREENLAND',404:'INVALID: GUAM',730:'INVALID: INTERNATIONAL WATERS',731:'INVALID: JOHNSON ISLAND',
471:'INVALID: MARIANA ISLANDS, NORTHERN',737:'INVALID: MIDWAY ISLANDS',753:'INVALID: MINOR OUTLYING ISLANDS - USA',
740:'INVALID: NEUTRAL ZONE (S. ARABIA/IRAQ)',710:'INVALID: NON-QUOTA IMMIGRANT',505:'INVALID: PUERTO RICO',
0:'INVALID: STATELESS',705:'INVALID: STATELESS',583:'INVALID: UNITED STATES',407:'INVALID: UNITED STATES',
999:'INVALID: UNKNOWN',239:'INVALID: UNKNOWN COUNTRY',134:'INVALID: USSR',506:'INVALID: U.S. VIRGIN ISLANDS',
755:'INVALID: WAKE ISLAND',311:'Collapsed Tanzania (should not show)',741:'Collapsed Curacao (should not show)',
54:'No Country Code (54)',100:'No Country Code (100)',187:'No Country Code (187)',190:'No Country Code (190)',
200:'No Country Code (200)',219:'No Country Code (219)',238:'No Country Code (238)',277:'No Country Code (277)',
293:'No Country Code (293)',300:'No Country Code (300)',319:'No Country Code (319)',365:'No Country Code (365)',
395:'No Country Code (395)',400:'No Country Code (400)',485:'No Country Code (485)',503:'No Country Code (503)',
589:'No Country Code (589)',592:'No Country Code (592)',791:'No Country Code (791)',849:'No Country Code (849)',
914:'No Country Code (914)',944:'No Country Code (944)',996:'No Country Code (996)',125.0:"No record",
133.0:"No record",147.0:"No record",148.0:"No record",180.0:"No record",247.0:"No record",248.0:"No record",
252.0:"No record",254.0:"No record",267.0:"No record",374.0:"No record",406.0:"No record",578.0:"No record",
580.0:"No record",714.0:"No record",718.0:"No record",727.0:"No record",728.0:"No record",733.0:"No record",
734.0:"No record",746.0:"No record",751.0:"No record",752.0:"No record",756.0:"No record",763.0:"No record",
764.0:"No record",765.0:"No record",766.0:"No record",769.0:"No record",770.0:"No record"}


port_of_entry_code = {'ALC':'ALCAN, AK','ANC':'ANCHORAGE, AK','BAR':'BAKER AAF - BAKER ISLAND, AK',
'DAC':'DALTONS CACHE, AK','PIZ':'DEW STATION PT LAY DEW, AK','DTH':'DUTCH HARBOR, AK','EGL':'EAGLE, AK',
'FRB':'FAIRBANKS, AK','HOM':'HOMER, AK','HYD':'HYDER, AK','JUN':'JUNEAU, AK','5KE':'KETCHIKAN, AK',
'KET':'KETCHIKAN, AK','MOS':'MOSES POINT INTERMEDIATE, AK','NIK':'NIKISKI, AK','NOM':'NOM, AK',
'PKC':'POKER CREEK, AK','ORI':'PORT LIONS SPB, AK','SKA':'SKAGWAY, AK','SNP':'ST. PAUL ISLAND, AK',
'TKI':'TOKEEN, AK','WRA':'WRANGELL, AK','HSV':'MADISON COUNTY - HUNTSVILLE, AL','MOB':'MOBILE, AL',
'LIA':'LITTLE ROCK, AR (BPS)','ROG':'ROGERS ARPT, AR','DOU':'DOUGLAS, AZ','LUK':'LUKEVILLE, AZ',
'MAP':'MARIPOSA AZ','NAC':'NACO, AZ','NOG':'NOGALES, AZ','PHO':'PHOENIX, AZ','POR':'PORTAL, AZ',
'SLU':'SAN LUIS, AZ','SAS':'SASABE, AZ','TUC':'TUCSON, AZ','YUI':'YUMA, AZ' ,'AND':'ANDRADE, CA',
'BUR':'BURBANK, CA','CAL':'CALEXICO, CA','CAO':'CAMPO, CA','FRE':'FRESNO, CA','ICP':'IMPERIAL COUNTY, CA',
'LNB':'LONG BEACH, CA','LOS':'LOS ANGELES, CA','BFL':'MEADOWS FIELD - BAKERSFIELD, CA','OAK':'OAKLAND, CA',
'ONT':'ONTARIO, CA','OTM':'OTAY MESA, CA','BLT':'PACIFIC, HWY. STATION, CA','PSP':'PALM SPRINGS, CA',
'SAC':'SACRAMENTO, CA','SLS':'SALINAS, CA (BPS)','SDP':'SAN DIEGO, CA','SFR':'SAN FRANCISCO, CA',
'SNJ':'SAN JOSE, CA','SLO':'SAN LUIS OBISPO, CA','SLI':'SAN LUIS OBISPO, CA (BPS)','SPC':'SAN PEDRO, CA',
'SYS':'SAN YSIDRO, CA','SAA':'SANTA ANA, CA','STO':'STOCKTON, CA (BPS)','TEC':'TECATE, CA',
'TRV':'TRAVIS-AFB, CA','APA':'ARAPAHOE COUNTY, CO','ASE':'ASPEN, CO #ARPT','COS':'COLORADO SPRINGS, CO',
'DEN':'DENVER, CO','DRO':'LA PLATA - DURANGO, CO','BDL':'BRADLEY INTERNATIONAL, CT','BGC':'BRIDGEPORT, CT',
'GRT':'GROTON, CT','HAR':'HARTFORD, CT','NWH':'NEW HAVEN, CT','NWL':'NEW LONDON, CT',
'TST':'NEWINGTON DATA CENTER TEST, CT','WAS':'WASHINGTON DC','DOV':'DOVER AFB, DE','DVD':'DOVER-AFB, DE',
'WLL':'WILMINGTON, DE','BOC':'BOCAGRANDE, FL','SRQ':'BRADENTON - SARASOTA, FL','CAN':'CAPE CANAVERAL, FL',
'DAB':'DAYTONA BEACH INTERNATIONAL, FL','FRN':'FERNANDINA, FL','FTL':'FORT LAUDERDALE, FL',
'FMY':'FORT MYERS, FL','FPF':'FORT PIERCE, FL','HUR':'HURLBURT FIELD, FL',
'GNV':'J R ALISON MUNI - GAINESVILLE, FL','JAC':'JACKSONVILLE, FL','KEY':'KEY WEST, FL',
'LEE':'LEESBURG MUNICIPAL AIRPORT, FL','MLB':'MELBOURNE, FL','MIA':'MIAMI, FL','APF':'NAPLES, FL #ARPT',
'OPF':'OPA LOCKA, FL','ORL':'ORLANDO, FL','PAN':'PANAMA CITY, FL','PEN':'PENSACOLA, FL',
'PCF':'PORT CANAVERAL, FL','PEV':'PORT EVERGLADES, FL','PSJ':'PORT ST JOE, FL','SFB':'SANFORD, FL',
'SGJ':'ST AUGUSTINE ARPT, FL','SAU':'ST AUGUSTINE, FL','FPR':'ST LUCIE COUNTY, FL','SPE':'ST PETERSBURG, FL',
'TAM':'TAMPA, FL','WPB':'WEST PALM BEACH, FL','ATL':'ATLANTA, GA','BRU':'BRUNSWICK, GA',
'AGS':'BUSH FIELD - AUGUSTA, GA','SAV':'SAVANNAH, GA','AGA':'AGANA, GU','HHW':'HONOLULU, HI',
'OGG':'KAHULUI - MAUI, HI','KOA':'KEAHOLE-KONA, HI','LIH':'LIHUE, HI','CID':'CEDAR RAPIDS/IOWA CITY, IA',
'DSM':'DES MOINES, IA','BOI':'AIR TERM. (GOWEN FLD) BOISE, ID','EPI':'EASTPORT, ID',
'IDA':'FANNING FIELD - IDAHO FALLS, ID','PTL':'PORTHILL, ID','SPI':'CAPITAL - SPRINGFIELD, IL','CHI':'CHICAGO, IL',
'DPA':'DUPAGE COUNTY, IL','PIA':'GREATER PEORIA, IL','RFD':'GREATER ROCKFORD, IL',
'UGN':'MEMORIAL - WAUKEGAN, IL','GAR':'GARY, IN','HMM':'HAMMOND, IN','INP':'INDIANAPOLIS, IN',
'MRL':'MERRILLVILLE, IN','SBN':'SOUTH BEND, IN','ICT':'MID-CONTINENT - WITCHITA, KS',
'LEX':'BLUE GRASS - LEXINGTON, KY','LOU':'LOUISVILLE, KY','BTN':'BATON ROUGE, LA','LKC':'LAKE CHARLES, LA',
'LAK':'LAKE CHARLES, LA (BPS)','MLU':'MONROE, LA','MGC':'MORGAN CITY, LA','NOL':'NEW ORLEANS, LA',
'BOS':'BOSTON, MA','GLO':'GLOUCESTER, MA','BED':'HANSCOM FIELD - BEDFORD, MA','LYN':'LYNDEN, WA',
'ADW':'ANDREWS AFB, MD','BAL':'BALTIMORE, MD','MKG':'MUSKEGON, MD','PAX':'PATUXENT RIVER, MD',
'BGM':'BANGOR, ME','BOO':'BOOTHBAY HARBOR, ME','BWM':'BRIDGEWATER, ME','BCK':'BUCKPORT, ME','CLS':'CALAIS, ME',
'CRB':'CARIBOU, ME','COB':'COBURN GORE, ME','EST':'EASTCOURT, ME','EPT':'EASTPORT MUNICIPAL, ME',
'EPM':'EASTPORT, ME','FOR':'FOREST CITY, ME','FTF':'FORT FAIRFIELD, ME','FTK':'FORT KENT, ME','HML':'HAMIIN, ME',
'HTM':'HOULTON, ME','JKM':'JACKMAN, ME','KAL':'KALISPEL, MT','LIM':'LIMESTONE, ME','LUB':'LUBEC, ME',
'MAD':'MADAWASKA, ME','POM':'PORTLAND, ME','RGM':'RANGELEY, ME (BPS)','SBR':'SOUTH BREWER, ME',
'SRL':'ST AURELIE, ME','SPA':'ST PAMPILE, ME','VNB':'VAN BUREN, ME','VCB':'VANCEBORO, ME','AGN':'ALGONAC, MI',
'ALP':'ALPENA, MI','BCY':'BAY CITY, MI','DET':'DETROIT, MI','GRP':'GRAND RAPIDS, MI','GRO':'GROSSE ISLE, MI',
'ISL':'ISLE ROYALE, MI','MRC':'MARINE CITY, MI','MRY':'MARYSVILLE, MI','PTK':'OAKLAND COUNTY - PONTIAC, MI',
'PHU':'PORT HURON, MI','RBT':'ROBERTS LANDING, MI','SAG':'SAGINAW, MI','SSM':'SAULT STE. MARIE, MI',
'SCL':'ST CLAIR, MI','YIP':'WILLOW RUN - YPSILANTI, MI','BAU':'BAUDETTE, MN','CAR':'CARIBOU MUNICIPAL AIRPORT, MN',
'GTF':'Collapsed into INT, MN','INL':'Collapsed into INT, MN','CRA':'CRANE LAKE, MN',
'MIC':'CRYSTAL MUNICIPAL AIRPORT, MN','DUL':'DULUTH, MN','ELY':'ELY, MN','GPM':'GRAND PORTAGE, MN',
'SVC':'GRANT COUNTY - SILVER CITY, MN','INT':"INT'L FALLS, MN",'LAN':'LANCASTER, MN','MSP':'MINN./ST PAUL, MN',
'LIN':'NORTHERN SVC CENTER, MN','NOY':'NOYES, MN','PIN':'PINE CREEK, MN','48Y':'PINECREEK BORDER ARPT, MN',
'RAN':'RAINER, MN','RST':'ROCHESTER, MN','ROS':'ROSEAU, MN','SPM':'ST PAUL, MN','WSB':'WARROAD INTL, SPB, MN',
'WAR':'WARROAD, MN','KAN':'KANSAS CITY, MO','SGF':'SPRINGFIELD-BRANSON, MO','STL':'ST LOUIS, MO',
'WHI':'WHITETAIL, MT','WHM':'WILD HORSE, MT','GPT':'BILOXI REGIONAL, MS','GTR':'GOLDEN TRIANGLE LOWNDES CNTY, MS',
'GUL':'GULFPORT, MS','PAS':'PASCAGOULA, MS','JAN':'THOMPSON FIELD - JACKSON, MS','BIL':'BILLINGS, MT',
'BTM':'BUTTE, MT','CHF':'CHIEF MT, MT','CTB':'CUT BANK MUNICIPAL, MT','CUT':'CUT BANK, MT',
'DLB':'DEL BONITA, MT','EUR':'EUREKA, MT (BPS)','BZN':'GALLATIN FIELD - BOZEMAN, MT',
'FCA':'GLACIER NATIONAL PARK, MT','GGW':'GLASGOW, MT','GRE':'GREAT FALLS, MT','HVR':'HAVRE, MT',
'HEL':'HELENA, MT','LWT':'LEWISTON, MT','MGM':'MORGAN, MT','OPH':'OPHEIM, MT','PIE':'PIEGAN, MT',
'RAY':'RAYMOND, MT','ROO':'ROOSVILLE, MT','SCO':'SCOBEY, MT','SWE':'SWEETGTASS, MT','TRL':'TRIAL CREEK, MT',
'TUR':'TURNER, MT','WCM':'WILLOW CREEK, MT','CLT':'CHARLOTTE, NC','FAY':'FAYETTEVILLE, NC',
'MRH':'MOREHEAD CITY, NC','FOP':'MORRIS FIELDS AAF, NC','GSO':'PIEDMONT TRIAD INTL AIRPORT, NC',
'RDU':'RALEIGH/DURHAM, NC','SSC':'SHAW AFB - SUMTER, NC','WIL':'WILMINGTON, NC','AMB':'AMBROSE, ND',
'ANT':'ANTLER, ND','CRY':'CARBURY, ND','DNS':'DUNSEITH, ND','FAR':'FARGO, ND','FRT':'FORTUNA, ND',
'GRF':'GRAND FORKS, ND','HNN':'HANNAH, ND','HNS':'HANSBORO, ND','MAI':'MAIDA, ND','MND':'MINOT, ND',
'NEC':'NECHE, ND','NOO':'NOONAN, ND','NRG':'NORTHGATE, ND','PEM':'PEMBINA, ND','SAR':'SARLES, ND',
'SHR':'SHERWOOD, ND','SJO':'ST JOHN, ND','WAL':'WALHALLA, ND','WHO':'WESTHOPE, ND','WND':'WILLISTON, ND',
'OMA':'OMAHA, NE','LEB':'LEBANON, NH','MHT':'MANCHESTER, NH','PNH':'PITTSBURG, NH','PSM':'PORTSMOUTH, NH',
'BYO':'BAYONNE, NJ','CNJ':'CAMDEN, NJ','HOB':'HOBOKEN, NJ','JER':'JERSEY CITY, NJ',
'WRI':'MC GUIRE AFB - WRIGHTSOWN, NJ','MMU':'MORRISTOWN, NJ','NEW':'NEWARK/TETERBORO, NJ',
'PER':'PERTH AMBOY, NJ','ACY':'POMONA FIELD - ATLANTIC CITY, NJ','ALA':'ALAMAGORDO, NM (BPS)',
'ABQ':'ALBUQUERQUE, NM','ANP':'ANTELOPE WELLS, NM','CRL':'CARLSBAD, NM','COL':'COLUMBUS, NM',
'CDD':'CRANE LAKE - ST. LOUIS CNTY, NM','DNM':'DEMING, NM (BPS)','LAS':'LAS CRUCES, NM',
'LOB':'LORDSBURG, NM (BPS)','RUI':'RUIDOSO, NM','STR':'SANTA TERESA, NM','RNO':'CANNON INTL - RENO/TAHOE, NV',
'FLX':'FALLON MUNICIPAL AIRPORT, NV','LVG':'LAS VEGAS, NV','REN':'RENO, NV','ALB':'ALBANY, NY',
'AXB':'ALEXANDRIA BAY, NY','BUF':'BUFFALO, NY','CNH':'CANNON CORNERS, NY','CAP':'CAPE VINCENT, NY',
'CHM':'CHAMPLAIN, NY','CHT':'CHATEAUGAY, NY','CLA':'CLAYTON, NY','FTC':'FORT COVINGTON, NY',
'LAG':'LA GUARDIA, NY','LEW':'LEWISTON, NY','MAS':'MASSENA, NY','MAG':'MCGUIRE AFB, NY','MOO':'MOORES, NY',
'MRR':'MORRISTOWN, NY','NYC':'NEW YORK, NY','NIA':'NIAGARA FALLS, NY','OGD':'OGDENSBURG, NY',
'OSW':'OSWEGO, NY','ELM':'REGIONAL ARPT - HORSEHEAD, NY','ROC':'ROCHESTER, NY','ROU':'ROUSES POINT, NY',
'SWF':'STEWART - ORANGE CNTY, NY','SYR':'SYRACUSE, NY','THO':'THOUSAND ISLAND BRIDGE, NY',
'TRO':'TROUT RIVER, NY','WAT':'WATERTOWN, NY','HPN':'WESTCHESTER - WHITE PLAINS, NY','WRB':'WHIRLPOOL BRIDGE, NY',
'YOU':'YOUNGSTOWN, NY','AKR':'AKRON, OH','ATB':'ASHTABULA, OH','CIN':'CINCINNATI, OH','CLE':'CLEVELAND, OH',
'CLM':'COLUMBUS, OH','LOR':'LORAIN, OH','MBO':'MARBLE HEADS, OH','SDY':'SANDUSKY, OH','TOL':'TOLEDO, OH',
'OKC':'OKLAHOMA CITY, OK','TUL':'TULSA, OK','AST':'ASTORIA, OR','COO':'COOS BAY, OR','HIO':'HILLSBORO, OR',
'MED':'MEDFORD, OR','NPT':'NEWPORT, OR','POO':'PORTLAND, OR','PUT':'PUT-IN-BAY, OH',
'RDM':'ROBERTS FIELDS - REDMOND, OR','ERI':'ERIE, PA','MDT':'HARRISBURG, PA','HSB':'HARRISONBURG, PA',
'PHI':'PHILADELPHIA, PA','PIT':'PITTSBURG, PA','AGU':'AGUADILLA, PR','BQN':'BORINQUEN - AGUADILLO, PR',
'JCP':'CULEBRA - BENJAMIN RIVERA, PR','ENS':'ENSENADA, PR','FAJ':'FAJARDO, PR','HUM':'HUMACAO, PR',
'JOB':'JOBOS, PR','MAY':'MAYAGUEZ, PR','PON':'PONCE, PR','PSE':'PONCE-MERCEDITA, PR','SAJ':'SAN JUAN, PR',
'VQS':'VIEQUES-ARPT, PR','PRO':'PROVIDENCE, RI','PVD':'THEODORE FRANCIS - WARWICK, RI','CHL':'CHARLESTON, SC',
'CAE':'COLUMBIA, SC #ARPT','GEO':'GEORGETOWN, SC','GSP':'GREENVILLE, SC','GRR':'GREER, SC','MYR':'MYRTLE BEACH, SC',
'SPF':'BLACK HILLS, SPEARFISH, SD','HON':'HOWES REGIONAL ARPT - HURON, SD','SAI':'SAIPAN, SPN',
'TYS':'MC GHEE TYSON - ALCOA, TN','MEM':'MEMPHIS, TN','NSV':'NASHVILLE, TN','TRI':'TRI CITY ARPT, TN',
'ADS':'ADDISON AIRPORT- ADDISON, TX','ADT':'AMISTAD DAM, TX','ANZ':'ANZALDUAS, TX','AUS':'AUSTIN, TX',
'BEA':'BEAUMONT, TX','BBP':'BIG BEND PARK, TX (BPS)','SCC':'BP SPEC COORD. CTR, TX','BTC':'BP TACTICAL UNIT, TX',
'BOA':'BRIDGE OF AMERICAS, TX','BRO':'BROWNSVILLE, TX','CRP':'CORPUS CHRISTI, TX','DAL':'DALLAS, TX',
'DLR':'DEL RIO, TX','DNA':'DONNA, TX','EGP':'EAGLE PASS, TX','ELP':'EL PASO, TX','FAB':'FABENS, TX',
'FAL':'FALCON HEIGHTS, TX','FTH':'FORT HANCOCK, TX','AFW':'FORT WORTH ALLIANCE, TX','FPT':'FREEPORT, TX',
'GAL':'GALVESTON, TX','HLG':'HARLINGEN, TX','HID':'HIDALGO, TX','HOU':'HOUSTON, TX',
'SGR':'HULL FIELD, SUGAR LAND ARPT, TX','LLB':'JUAREZ-LINCOLN BRIDGE, TX','LCB':'LAREDO COLUMBIA BRIDGE, TX',
'LRN':'LAREDO NORTH, TX','LAR':'LAREDO, TX','LSE':'LOS EBANOS, TX','IND':'LOS INDIOS, TX',
'LOI':'LOS INDIOS, TX','MRS':'MARFA, TX (BPS)','MCA':'MCALLEN, TX','MAF':'ODESSA REGIONAL, TX',
'PDN':'PASO DEL NORTE,TX','PBB':'PEACE BRIDGE, NY','PHR':'PHARR, TX','PAR':'PORT ARTHUR, TX',
'ISB':'PORT ISABEL, TX','POE':'PORT OF EL PASO, TX','PRE':'PRESIDIO, TX','PGR':'PROGRESO, TX',
'RIO':'RIO GRANDE CITY, TX','ROM':'ROMA, TX','SNA':'SAN ANTONIO, TX','SNN':'SANDERSON, TX',
'VIB':'VETERAN INTL BRIDGE, TX','YSL':'YSLETA, TX','CHA':'CHARLOTTE AMALIE, VI','CHR':'CHRISTIANSTED, VI',
'CRU':'CRUZ BAY, ST JOHN, VI','FRK':'FREDERIKSTED, VI','STT':'ST THOMAS, VI','LGU':'CACHE AIRPORT - LOGAN, UT',
'SLC':'SALT LAKE CITY, UT','CHO':'ALBEMARLE CHARLOTTESVILLE, VA','DAA':'DAVISON AAF - FAIRFAX CNTY, VA',
'HOP':'HOPEWELL, VA','HEF':'MANASSAS, VA #ARPT','NWN':'NEWPORT, VA','NOR':'NORFOLK, VA',
'RCM':'RICHMOND, VA','ABS':'ALBURG SPRINGS, VT','ABG':'ALBURG, VT','BEB':'BEEBE PLAIN, VT',
'BEE':'BEECHER FALLS, VT','BRG':'BURLINGTON, VT','CNA':'CANAAN, VT','DER':'DERBY LINE, VT (I-91)',
'DLV':'DERBY LINE, VT (RT. 5)','ERC':'EAST RICHFORD, VT','HIG':'HIGHGATE SPRINGS, VT',
'MOR':'MORSES LINE, VT','NPV':'NEWPORT, VT','NRT':'NORTH TROY, VT','NRN':'NORTON, VT',
'PIV':'PINNACLE ROAD, VT','RIF':'RICHFORT, VT','STA':'ST ALBANS, VT','SWB':'SWANTON, VT (BP - SECTOR HQ)',
'WBE':'WEST BERKSHIRE, VT','ABE':'ABERDEEN, WA','ANA':'ANACORTES, WA','BEL':'BELLINGHAM, WA',
'BLI':'BELLINGHAM, WASHINGTON #INTL','BLA':'BLAINE, WA','BWA':'BOUNDARY, WA','CUR':'CURLEW, WA (BPS)',
'DVL':'DANVILLE, WA','EVE':'EVERETT, WA','FER':'FERRY, WA','FRI':'FRIDAY HARBOR, WA','FWA':'FRONTIER, WA',
'KLM':'KALAMA, WA','LAU':'LAURIER, WA','LON':'LONGVIEW, WA','MET':'METALINE FALLS, WA',
'MWH':'MOSES LAKE GRANT COUNTY ARPT, WA','NEA':'NEAH BAY, WA','NIG':'NIGHTHAWK, WA','OLY':'OLYMPIA, WA',
'ORO':'OROVILLE, WA','PWB':'PASCO, WA','PIR':'POINT ROBERTS, WA','PNG':'PORT ANGELES, WA',
'PTO':'PORT TOWNSEND, WA','SEA':'SEATTLE, WA','SPO':'SPOKANE, WA','SUM':'SUMAS, WA','TAC':'TACOMA, WA',
'PSC':'TRI-CITIES - PASCO, WA','VAN':'VANCOUVER, WA','AGM':'ALGOMA, WI','BAY':'BAYFIELD, WI',
'GRB':'GREEN BAY, WI','MNW':'MANITOWOC, WI','MIL':'MILWAUKEE, WI','MSN':'TRUAX FIELD - DANE COUNTY, WI',
'CHS':'CHARLESTON, WV','CLK':'CLARKSBURG, WV','BLF':'MERCER COUNTY, WV','CSP':'CASPER, WY',
'XXX':'NOT REPORTED/UNKNOWN','888':'UNIDENTIFED AIR / SEAPORT','UNK':'UNKNOWN POE','CLG':'CALGARY, CANADA',
'EDA':'EDMONTON, CANADA','YHC':'HAKAI PASS, CANADA','HAL':'Halifax, NS, Canada','MON':'MONTREAL, CANADA',
'OTT':'OTTAWA, CANADA','YXE':'SASKATOON, CANADA','TOR':'TORONTO, CANADA','VCV':'VANCOUVER, CANADA',
'VIC':'VICTORIA, CANADA','WIN':'WINNIPEG, CANADA','AMS':'AMSTERDAM-SCHIPHOL, NETHERLANDS',
'ARB':'ARUBA, NETH ANTILLES','BAN':'BANKOK, THAILAND','BEI':'BEICA #ARPT, ETHIOPIA',
'PEK':'BEIJING CAPITAL INTL, PRC','BDA':'KINDLEY FIELD, BERMUDA','BOG':'BOGOTA, EL DORADO #ARPT, COLOMBIA',
'EZE':'BUENOS AIRES, MINISTRO PIST, ARGENTINA','CUN':'CANCUN, MEXICO','CRQ':'CARAVELAS, BA #ARPT, BRAZIL',
'MVD':'CARRASCO, URUGUAY','DUB':'DUBLIN, IRELAND','FOU':'FOUGAMOU #ARPT, GABON','FBA':'FREEPORT, BAHAMAS',
'MTY':'GEN M. ESCOBEDO, Monterrey, MX','HMO':'GEN PESQUEIRA GARCIA, MX','GCM':'GRAND CAYMAN, CAYMAN ISLAND',
'GDL':'GUADALAJARA, MIGUEL HIDAL, MX','HAM':'HAMILTON, BERMUDA','ICN':'INCHON, SEOUL KOREA',
'IWA':'INVALID - IWAKUNI, JAPAN','CND':'KOGALNICEANU, ROMANIA','LAH':'LABUHA ARPT, INDONESIA',
'DUR':'LOUIS BOTHA, SOUTH AFRICA','MAL':'MANGOLE ARPT, INDONESIA','MDE':'MEDELLIN, COLOMBIA',
'MEX':'JUAREZ INTL, MEXICO CITY, MX','LHR':'MIDDLESEX, ENGLAND','NBO':'NAIROBI, KENYA',
'NAS':'NASSAU, BAHAMAS','NCA':'NORTH CAICOS, TURK & CAIMAN','PTY':'OMAR TORRIJOS, PANAMA',
'SPV':'PAPUA, NEW GUINEA','UIO':'QUITO (MARISCAL SUCR), ECUADOR','RIT':'ROME, ITALY',
'SNO':'SAKON NAKHON #ARPT, THAILAND','SLP':'SAN LUIS POTOSI #ARPT, MEXICO','SAN':'SAN SALVADOR, EL SALVADOR',
'SRO':'SANTANA RAMOS #ARPT, COLOMBIA','GRU':'GUARULHOS INTL, SAO PAULO, BRAZIL','SHA':'SHANNON, IRELAND',
'HIL':'SHILLAVO, ETHIOPIA','TOK':'TOROKINA #ARPT, PAPUA, NEW GUINEA','VER':'VERACRUZ, MEXICO',
'LGW':'WEST SUSSEX, ENGLAND','ZZZ':'MEXICO Land (Banco de Mexico)','CHN':'No PORT Code (CHN)',
'CNC':'CANNON CORNERS, NY','MAA':'Abu Dhabi','AG0':'MAGNOLIA, AR','BHM':'BAR HARBOR, ME','BHX':'BIRMINGHAM, AL',
'CAK':'AKRON, OH','FOK':'SUFFOLK COUNTY, NY','LND':'LANDER, WY','MAR':'MARFA, TX','MLI':'MOLINE, IL',
'RIV':'RIVERSIDE, CA','RME':'ROME, NY','VNY':'VAN NUYS, CA','YUM':'YUMA, AZ','FRG':'Collapsed (FOK) 06/15',
'HRL':'Collapsed (HLG) 06/15','ISP':'Collapsed (FOK) 06/15','JSJ':'Collapsed (SAJ) 06/15',
'BUS':'Collapsed (BUF) 06/15','IAG':'Collapsed (NIA) 06/15','PHN':'Collapsed (PHU) 06/15',
'STN':'Collapsed (STR) 06/15','VMB':'Collapsed (VNB) 06/15','T01':'Collapsed (SEA) 06/15',
'PHF':'No PORT Code (PHF)','DRV':'No PORT Code (DRV)','FTB':'No PORT Code (FTB)','GAC':'No PORT Code (GAC)',
'GMT':'No PORT Code (GMT)','JFA':'No PORT Code (JFA)','JMZ':'No PORT Code (JMZ)','NC8':'No PORT Code (NC8)',
'NYL':'No PORT Code (NYL)','OAI':'No PORT Code (OAI)','PCW':'No PORT Code (PCW)','WA5':'No PORT Code (WAS)',
'WTR':'No PORT Code (WTR)','X96':'No PORT Code (X96)','XNA':'No PORT Code (XNA)','YGF':'No PORT Code (YGF)',
'5T6':'No PORT Code (5T6)','060':'No PORT Code (60)','SP0':'No PORT Code (SP0)','W55':'No PORT Code (W55)',
'X44':'No PORT Code (X44)','AUH':'No PORT Code (AUH)','RYY':'No PORT Code (RYY)','SUS':'No PORT Code (SUS)',
'74S':'No PORT Code (74S)','ATW':'No PORT Code (ATW)','CPX':'No PORT Code (CPX)','MTH':'No PORT Code (MTH)',
'PFN':'No PORT Code (PFN)','SCH':'No PORT Code (SCH)','ASI':'No PORT Code (ASI)','BKF':'No PORT Code (BKF)',
'DAY':'No PORT Code (DAY)','Y62':'No PORT Code (Y62)','AG':'No PORT Code (AG)','BCM':'No PORT Code (BCM)',
'DEC':'No PORT Code (DEC)','PLB':'No PORT Code (PLB)','CXO':'No PORT Code (CXO)','JBQ':'No PORT Code (JBQ)',
'JIG':'No PORT Code (JIG)','OGS':'No PORT Code (OGS)','TIW':'No PORT Code (TIW)','OTS':'No PORT Code (OTS)',
'AMT':'No PORT Code (AMT)','EGE':'No PORT Code (EGE)','GPI':'No PORT Code (GPI)','NGL':'No PORT Code (NGL)',
'OLM':'No PORT Code (OLM)','.GA':'No PORT Code (.GA)','CLX':'No PORT Code (CLX)','CP ':'No PORT Code (CP)',
'FSC':'No PORT Code (FSC)','NK':'No PORT Code (NK)','ADU':'No PORT Code (ADU)','AKT':'No PORT Code (AKT)',
'LIT':'No PORT Code (LIT)','A2A':'No PORT Code (A2A)','OSN':'No PORT Code (OSN)'}



mode_of_entry_code = {0:'No record',1:'Air',2:'Sea',3:'Land',9:'Not reported'}
visa_code = {1:'Business',2:'Pleasure',3:'Student'}




"""
        Description:
            This file reads the immigration data using the
            spark read method and performs data cleaning, processing,
            and finally saves the cleaned data as parquet file to S3.


            spark : spark session.
            output_data: the output root directory to the s3 bucket.



"""

output_data = "s3a://charlesprojects-sink/"


df_immigration = spark.read.csv('s3a://charlesprojects-source/immigrationData.csv',schema=paramSchema,\
                                    header=True)

df_immigration = df_immigration.na.fill({'i94mode':0,'i94addr':'Nil','i94bir':0,'dtadfile':'Nil',
                'visapost':'Nil','occup':'Nil','entdepa':'Nil','entdepd':'Nil','entdepu':'Nil','matflag':'Nil',
                'biryear':0,'dtaddto':'Nil','gender':'Nil','insnum':'Nil','airline':'Nil','fltno':'Nil'})


replaceCountry = udf(lambda x: country_code[x])

replaceVisa  = udf(lambda x: visa_code[x])

replaceMode = udf(lambda x: mode_of_entry_code[x])

#replaceAddress = udf(lambda x: address_code[x])

replacePort = udf(lambda x: port_of_entry_code[x])

get_date = udf(lambda x: (datetime(1960, 1, 1).date() + timedelta(x)).isoformat() if x else None )


df_immigration = df_immigration.withColumn('i94citizenship',replaceCountry(df_immigration['i94cit'])).drop("i94cit").\
                withColumn('i94resident',replaceCountry(df_immigration['i94res'])).drop("i94res","_c0").\
                withColumn('Visa',replaceVisa(df_immigration['i94visa'])).drop("i94visa").\
                withColumn('i94PortOfEntry',replacePort(df_immigration['i94port'])).drop("i94port").\
                withColumn('i94modeOfEntry',replaceMode(df_immigration['i94mode'])).drop("i94mode").\
                withColumn('i94AddressTo',trim(split("i94addr"," ")[0])).drop("i94addr")

df_immigration = df_immigration.withColumn('i94PortOfEntryCity',F.lower(trim(split("i94PortOfEntry",",")[0]))).\
                    withColumn('i94Port',F.upper(trim(split("i94PortOfEntry",",")[1]))).\
                    withColumn('i94PortOfEntryState',split("i94Port"," ")[0]).drop('i94Port').\
                    withColumn('arrdate',get_date("arrdate")).drop("i94PortOfEntry").\
                    withColumn('arrivedate',col("arrdate").cast('date')).drop("i94PortOfEntry",'arrdate').\
                    withColumn('ddate',get_date("depdate")).drop("arrdate","depdate").\
                    withColumn('departdate',col("ddate").cast('date')).drop("arrdate","depdate",'ddate').\
                    withColumn('i94year',col('i94yr').cast('integer')).\
                    withColumn('i94month',col('i94mon').cast('integer')).drop("i94yr","i94mon").\
                    withColumn('i94age',col('i94bir').cast('integer')).drop("i94bir").\
                    withColumn('i94birthyear',col('biryear').cast('integer')).drop("biryear","count")


df_immigration = df_immigration.where(df_immigration.i94modeOfEntry == 'Air').where(df_immigration.i94age >= 0)

df_immigration = df_immigration.na.fill({'i94PortOfEntryState':'Nil'})

df_immigration = df_immigration.dropDuplicates(['cicid'])

df_immigration = df_immigration.select('cicid','i94citizenship','i94resident','Visa','i94modeOfEntry',
            'i94AddressTo','i94PortOfEntryCity','i94PortOfEntryState','gender','arrivedate','departdate',
            'i94year','i94month','i94birthyear','i94age','fltno','airline','visatype','visapost','dtadfile',
            'occup','entdepa','entdepd','entdepu','matflag','dtaddto','insnum','admnum')

# write processed data as parquet file to S3
df_immigration.write.mode("overwrite").partitionBy("i94year","i94month").\
        parquet(os.path.join(output_data,"immigrationData"))
