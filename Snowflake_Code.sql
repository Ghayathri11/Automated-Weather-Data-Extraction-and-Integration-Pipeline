CREATE OR REPLACE FILE FORMAT DW_COURSE_P1.OWM_ETL.JSONFORMAT
    TYPE = JSON;

CREATE OR REPLACE STORAGE INTEGRATION S3_CONNECT
    TYPE=EXTERNAL_STAGE
    STORAGE_PROVIDER='S3'
    ENABLED=TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::655966661751:role/snowflake-s3-connection'
    STORAGE_ALLOWED_LOCATIONS = ('s3://owm-etl-project')
    COMMENT = 'Creating s3 connection' 

DESC STORAGE INTEGRATION S3_CONNECT;

CREATE OR REPLACE STAGE DW_COURSE_P1.OWM_ETL.JSON_FOLDER
    URL='s3://owm-etl-project/transformed-data/'
    STORAGE_INTEGRATION=S3_CONNECT
    FILE_FORMAT = DW_COURSE_P1.OWM_ETL.JSONFORMAT

LIST @DW_COURSE_P1.OWM_ETL.JSON_FOLDER

CREATE OR REPLACE TABLE DW_COURSE_P1.OWM_ETL.JSON_RAW(
    raw_file variant
);

create or replace pipe DW_COURSE_P1.OWM_ETL.weather_pipe
auto_ingest=true
AS
COPY INTO DW_COURSE_P1.OWM_ETL.JSON_RAW
FROM @DW_COURSE_P1.OWM_ETL.JSON_FOLDER

SELECT * FROM DW_COURSE_P1.OWM_ETL.JSON_RAW

CREATE OR REPLACE TABLE WEATHER_TABLE AS
SELECT $1:country::string Country,
        $1:status::string Status,
        $1:wind_speed::decimal(10,2) Wind_Speed,
        $1:"Pressure(Hg)"::decimal(10,2) Pressure,
        $1:"temperature(C)"::decimal(10,2) Temperature,
        $1:max_temp::decimal(10,2) Max_temp,
        $1:min_temp::decimal(10,2) min_temp,
        $1:humidity::decimal(10,2) humidity,
        $1:sunrise::datetime sunrise_time,
        $1:sunset::datetime sunset_time
from DW_COURSE_P1.OWM_ETL.JSON_RAW

SELECT * FROM WEATHER_TABLE

COPY INTO DW_COURSE_P1.OWM_ETL.WEATHER_TABLE
FROM (SELECT $1:country::string Country,
        $1:status::string Status,
        $1:wind_speed::decimal(10,2) Wind_Speed,
        $1:"Pressure(Hg)"::decimal(10,2) Pressure,
        $1:"temperature(C)"::decimal(10,2) Temperature,
        $1:max_temp::decimal(10,2) Max_temp,
        $1:min_temp::decimal(10,2) min_temp,
        $1:humidity::decimal(10,2) humidity,
        $1:sunrise::datetime sunrise_time,
        $1:sunset::datetime sunset_time
from DW_COURSE_P1.OWM_ETL.JSON_RAW)

TRUNCATE TABLE DW_COURSE_P1.OWM_ETL.WEATHER_TABLE

create or replace pipe DW_COURSE_P1.OWM_ETL.weather_pipe
auto_ingest=true
AS
COPY INTO DW_COURSE_P1.OWM_ETL.WEATHER_TABLE
FROM (
SELECT $1:country::string Country,
        $1:status::string Status,
        $1:wind_speed::decimal(10,2) Wind_Speed,
        $1:"Pressure(Hg)"::decimal(10,2) Pressure,
        $1:"temperature(C)"::decimal(10,2) Temperature,
        $1:max_temp::decimal(10,2) Max_temp,
        $1:min_temp::decimal(10,2) min_temp,
        $1:humidity::decimal(10,2) humidity,
        $1:sunrise::datetime sunrise_time,
        $1:sunset::datetime sunset_time
FROM @DW_COURSE_P1.OWM_ETL.JSON_FOLDER)

DESC PIPE DW_COURSE_P1.OWM_ETL.weather_pipe

SELECT * FROM WEATHER_TABLE

