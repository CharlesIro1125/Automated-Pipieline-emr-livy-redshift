class SqlQueries:



# insert into statement for the analytical Star-Schema


    dimStateDestination_insert = ("""(AddressTo,StateCode,State,MedianAge,
                                    MalePopulation,FemalePopulation,TotalPopulation,
                                    NumberOfVeterans,ForeignBorn,AverageHouseholdSize)
                                        SELECT DISTINCT i.i94AddressTo as AddressTo,d.StateCode,d.State,d.MedianAge,
                                            d.MalePopulation,d.FemalePopulation,d.TotalPopulation,d.NumberOfVeterans,
                                            d.ForeignBorn,d.AverageHouseholdSize
                                            FROM staging_immigration as i
                                            LEFT JOIN staging_demography as d ON (d.StateCode = i.i94AddressTo);
                                            """)

    dimTourist_insert = ("""(Tourist_id,gender,birthyear,citizenship,resident,age,occupation,
                                            Visa,visatype,flight_no,airline, issuanceNum , admissionNum)
                                 SELECT DISTINCT cicid as Tourist_id,gender,i94birthyear as birthyear,
                                     i94citizenship as citizenship,i94resident as resident,i94age as age,
                                     occup as occupation,Visa,visatype,fltno as flight_no,airline,
                                     insnum as issuanceNum,admnum as admissionNum
                                 FROM staging_immigration;
                                     """)


    dimAirport_insert = ("""(airport_code, portOfEntryCity,gps_code,iata_code,
                                local_code,name,type,iso_country,municipality,statecode,ElevationFt)
                                 SELECT DISTINCT a.ident as airport_code,
                                     i.i94PortOfEntryCity as portOfEntryCity,a.gps_code,
                                     a.iata_code,a.local_code,a.name,a.type,a.iso_country,
                                     a.municipality,a.statecode,a.ElevationFt
                                     FROM staging_immigration as i
                                     LEFT JOIN staging_airport as a ON (a.statecode = i.i94PortOfEntryCity);
                                     """)

    dimArriveDate_insert = ("""(arrivedate_id,year,month,day,
                            weekday,week,AverageTemperature,AverageTemperatureUncertainty)
                            SELECT DISTINCT cast(i.arrivedate AS text) AS arrivedate_id,
                                EXTRACT(year from (i.arrivedate)) AS year,EXTRACT(month from (i.arrivedate)) AS month,
                                EXTRACT(day from (i.arrivedate)) AS day,EXTRACT(weekday from (i.arrivedate)) AS weekday,
                                EXTRACT(week from (i.arrivedate)) AS week,l.AverageTemperature,
                                l.AverageTemperatureUncertainty
                            FROM staging_immigration as i
                            LEFT JOIN staging_landtemp as l ON (i.arrivedate = l.dt);
                            """)

    factArrivetheUsa_insert = ("""(Tourist_id,arrivedate_id,AddressTo,
                                portOfEntryCity,departdate,arrivalFlag,departureFlag,updateFlag,matchFlag,admittedDate)
                                SELECT DISTINCT cicid as Tourist_id,cast(arrivedate AS text) AS arrivedate_id,
                                    i94AddressTo as AddressTo,i94PortOfEntryCity as portOfEntryCity,departdate,
                                    entdepa as arrivalFlag,entdepd as departureFlag,
                                    entdepu as updateFlag,matflag as matchFlag,dtaddto as admittedDate
                                FROM staging_immigration;
                                """)




# QUERY LISTS



    insert_table_queries = [dimStateDestination_insert,dimTourist_insert,dimAirport_insert,dimArriveDate_insert,
                        factArrivetheUsa_insert]



#QUALITY CHECK

    test_duplicate = """SELECT {column},count({column}) AS duplicates FROM {table} group By {column}
                            HAVING count({column}) > 1;"""

    test_null = """SELECT {column} FROM {table} WHERE {column} IS NULL;"""

    test_invalid_record = """SELECT SUM(CASE WHEN CAST(arrivedate_id AS date) > departdate THEN 1 ELSE 0 END) AS
                    invalid_records FROM factArrivetheUsa;"""

    test_record_complete = """SELECT count(*) FROM {table};"""
