
--DROP TABLE IF EXISTS public.staging_immigration;
--DROP TABLE IF EXISTS public.staging_airport;
--DROP TABLE IF EXISTS public.staging_demography;
--DROP TABLE IF EXISTS public.staging_landtemp;
--DROP TABLE IF EXISTS public.factArrivetheUsa;
--DROP TABLE IF EXISTS public.dimStateDestination;
--DROP TABLE IF EXISTS public.dimAirport;
--DROP TABLE IF EXISTS public.dimTourist;
--DROP TABLE IF EXISTS public.dimArriveDate;
--DROP TABLE IF EXISTS public.temp_factArrivetheUsa;
--DROP TABLE IF EXISTS public.temp_dimStateDestination;
--DROP TABLE IF EXISTS public.temp_dimAirport;
--DROP TABLE IF EXISTS public.temp_dimTourist;
--DROP TABLE IF EXISTS public.temp_dimArriveDate;


-- begin the transaction

BEGIN;


CREATE TABLE IF NOT EXISTS public.staging_immigration (
                                cicid float distkey,
                                i94citizenship TEXT,
                                i94resident TEXT,
                                Visa varchar(20) ,
                                i94modeOfEntry varchar(20),
                                i94AddressTo varchar(10),
                                i94PortOfEntryCity varchar(40),
                                i94PortOfEntryState varchar(20),
                                gender varchar(10),
                                arrivedate date,
                                departdate date,
                                i94birthyear integer,
                                i94age integer,
                                fltno varchar(20),
                                airline varchar(10),
                                visatype varchar(10),
                                visapost varchar(10),
                                dtadfile varchar(10),
                                occup TEXT,
                                entdepa varchar(10),
                                entdepd varchar(10),
                                entdepu varchar(10),
                                matflag varchar(10),
                                dtaddto varchar(10),
                                insnum varchar(10),
                                admnum float,
                                PRIMARY KEY (cicid))
                                diststyle key;

CREATE TABLE IF NOT EXISTS public.staging_airport (
                                ident varchar(10),
                                type TEXT,
                                name TEXT,
                                iso_country varchar(10),
                                municipality TEXT,
                                gps_code varchar(10),
                                iata_code varchar(10),
                                local_code varchar(10),
                                statecode varchar(5),
                                latitude_deg varchar(25),
                                longitude_deg varchar(25),
                                coordinate varchar(50),
                                geocode varchar(400),
                                iso_region_state varchar(10),
                                ElevationFt integer,
                                City TEXT,
                                PRIMARY KEY (ident))
                                diststyle all;


CREATE TABLE IF NOT EXISTS public.staging_demography(
                                StateCode varchar(10),
                                State TEXT,
                                MedianAge float,
                                TotalPopulation bigint,
                                NumberOfVeterans bigint,
                                AverageHouseholdSize float,
                                ForeignBorn bigint,
                                FemalePopulation bigint,
                                MalePopulation bigint,
                                PRIMARY KEY (StateCode))
                                diststyle all;

CREATE TABLE IF NOT EXISTS public.staging_landtemp(
                                stagingtemp_id bigint,
                                dt date,
                                AverageTemperature float,
                                AverageTemperatureUncertainty float,
                                state varchar(20),
                                Country varchar(20),
                                PRIMARY KEY (stagingtemp_id))
                                diststyle all;





CREATE TABLE IF NOT EXISTS public.dimStateDestination (
                                AddressTo varchar(5) sortkey,
                                StateCode varchar(5),
                                State varchar(20),
                                MedianAge float,
                                MalePopulation integer,
                                FemalePopulation integer,
                                TotalPopulation integer,
                                NumberOfVeterans integer,
                                ForeignBorn integer,
                                AverageHouseholdSize float,
                                PRIMARY KEY (AddressTo))
                                diststyle all;

CREATE TABLE IF NOT EXISTS public.dimTourist (
                                Tourist_id float distkey,
                                gender varchar(5),
                                birthyear integer,
                                citizenship TEXT,
                                resident TEXT,
                                age integer,
                                occupation TEXT,
                                Visa varchar(20),
                                visatype varchar(10),
                                visapost varchar(10),
                                flight_no varchar(20),
                                airline varchar(10),
                                issuanceNum varchar(10),
                                admissionNum float,
                                PRIMARY KEY (Tourist_id))
                                diststyle key;


CREATE TABLE IF NOT EXISTS public.dimAirport (
                                airport_id bigint identity(0,1),
                                airport_code varchar(10),
                                portOfEntryCity varchar(30) UNIQUE NOT NULL sortkey,
                                gps_code varchar(5),
                                iata_code varchar(5),
                                local_code varchar(5),
                                name TEXT,
                                type TEXT,
                                iso_country varchar(5),
                                municipality varchar(5),
                                statecode varchar(5),
                                ElevationFt integer,
                                PRIMARY KEY (airport_id))
                                diststyle all;


CREATE TABLE IF NOT EXISTS public.dimArriveDate (
                            arrivedate_id varchar(20) sortkey,
                            year integer,
                            month integer,
                            day integer,
                            weekday integer,
                            week integer,
                            AverageTemperature float,
                            AverageTemperatureUncertainty float,
                            PRIMARY KEY (arrivedate_id))
                            diststyle all;



CREATE TABLE IF NOT EXISTS public.factArrivetheUsa (
                            arrive_id bigint GENERATED BY DEFAULT AS IDENTITY(0, 1),
                            Tourist_id float distkey,
                            arrivedate_id varchar(20) sortkey,
                            AddressTo varchar(5),
                            portOfEntryCity varchar(30),
                            departdate date,
                            arrivalFlag varchar(5),
                            departureFlag varchar(5),
                            updateFlag varchar(5),
                            matchFlag varchar(5),
                            admittedDate varchar(10),
                            PRIMARY KEY (arrive_id,Tourist_id,arrivedate_id),
                            foreign key(Tourist_id) references dimTourist(Tourist_id),
                            foreign key(portOfEntryCity) references dimAirport(portOfEntryCity),
                            foreign key(arrivedate_id) references dimArriveDate(arrivedate_id),
                            foreign key(AddressTo) references dimStateDestination(AddressTo))
                            diststyle key;


CREATE TABLE IF NOT EXISTS public.temp_dimStateDestination (
                            AddressTo varchar(5) sortkey,
                            StateCode varchar(5),
                            State varchar(20),
                            MedianAge float,
                            MalePopulation integer,
                            FemalePopulation integer,
                            TotalPopulation integer,
                            NumberOfVeterans integer,
                            ForeignBorn integer,
                            AverageHouseholdSize float,
                            PRIMARY KEY (AddressTo))
                            diststyle all;

CREATE TABLE IF NOT EXISTS public.temp_dimTourist (
                            Tourist_id float distkey,
                            gender varchar(5),
                            birthyear integer,
                            citizenship TEXT,
                            resident TEXT,
                            age integer,
                            occupation TEXT,
                            Visa varchar(20),
                            visatype varchar(10),
                            visapost varchar(10),
                            flight_no varchar(20),
                            airline varchar(10),
                            issuanceNum varchar(10),
                            admissionNum float,
                            PRIMARY KEY (Tourist_id))
                            diststyle key;


CREATE TABLE IF NOT EXISTS public.temp_dimAirport (
                            airport_id bigint identity(0,1),
                            airport_code varchar(10),
                            portOfEntryCity varchar(30) UNIQUE NOT NULL sortkey,
                            gps_code varchar(5),
                            iata_code varchar(5),
                            local_code varchar(5),
                            name TEXT,
                            type TEXT,
                            iso_country varchar(5),
                            municipality varchar(5),
                            statecode varchar(5),
                            ElevationFt integer,
                            PRIMARY KEY (airport_id))
                            diststyle all;


CREATE TABLE IF NOT EXISTS public.temp_dimArriveDate (
                            arrivedate_id varchar(20) sortkey,
                            year integer,
                            month integer,
                            day integer,
                            weekday integer,
                            week integer,
                            AverageTemperature float,
                            AverageTemperatureUncertainty float,
                            PRIMARY KEY (arrivedate_id))
                            diststyle all;



CREATE TABLE IF NOT EXISTS public.temp_factArrivetheUsa (
                           Tourist_id float distkey,
                           arrivedate_id varchar(20) sortkey,
                           AddressTo varchar(5),
                           portOfEntryCity varchar(30),
                           departdate date,
                           arrivalFlag varchar(5),
                           departureFlag varchar(5),
                           updateFlag varchar(5),
                           matchFlag varchar(5),
                           admittedDate varchar(10),
                           PRIMARY KEY (Tourist_id,arrivedate_id),
                           foreign key(Tourist_id) references temp_dimTourist(Tourist_id),
                           foreign key(portOfEntryCity) references temp_dimAirport(portOfEntryCity),
                           foreign key(arrivedate_id) references temp_dimArriveDate(arrivedate_id),
                           foreign key(AddressTo) references temp_dimStateDestination(AddressTo))
                           diststyle key;




COMMIT;
-- end the transaction
