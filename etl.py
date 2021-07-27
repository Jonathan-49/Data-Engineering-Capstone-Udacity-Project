"""
Extract, Transform and Load i94 immigration, airport, global temperature and city demographic data and write the output in Parquet format
"""

import configparser
import os
from datetime import datetime
from datetime import datetime, timedelta
import calendar
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql.types import DateType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import types as T
from pyspark.sql.functions import udf
from pyspark.sql import SparkSession

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

I94_INPUT_PATH = config['LOCAL']['I94_INPUT_PATH']
AIRPORTS_INPUT_PATH=config['LOCAL']['AIRPORTS_INPUT_PATH']
PORTS_INPUT_PATH=config['LOCAL']['PORTS_INPUT_PATH']
COUNTRY_INPUT_PATH=config['LOCAL']['COUNTRY_INPUT_PATH']
COUNTRY_TEMPERATURE_INPUT_PATH=config['LOCAL']['COUNTRY_TEMPERATURE_INPUT_PATH']
CITY_TEMPERATURE_INPUT_PATH=config['LOCAL']['CITY_TEMPERATURE_INPUT_PATH']
DEMO_INPUT_PATH=config['LOCAL']['DEMO_INPUT_PATH']
OUTPUT_PATH = config['LOCAL']['OUTPUT_PATH']

def create_spark_session():
    """
    Creates a Spark session
    :return: object of type 'pyspark.sql.session.SparkSession'
    """
    
    spark = SparkSession.builder.\
    config("spark.jars.repositories", "https://repos.spark-packages.org/").\
    config("spark.jars.packages", "saurfang:spark-sas7bdat:3.0.0-s_2.11").\
    enableHiveSupport().getOrCreate()
    #spark = SparkSession.builder.getOrCreate()
    return spark


def convert_datetime(num_days):
    """
    Converts a SAS format date; stored as days since 1/1/1960 to a datetime
    :param num_days: double: Number of days since 1/1/1960
    :return: datetime
    """
    try:
        start = datetime(1960, 1, 1)
        return start + timedelta(days=int(num_days))
    except:
        return None

    
def convert_i94mode(mode):
    """
    Translates a i94 travel mode code to a description
    :param mode: int i94 mode as an integer
    :return: mode description
    """
    if mode == 1:
        return "Air"
    elif mode == 2:
        return "Sea"
    elif mode == 3:
        return "Land"
    else:
        return "Unknown"


def convert_visatype(visa):
    """
    Translates a visa numeric code to a description
    :param visa: str
    :return: Visa description: str
    """
    if visa is None:
        return "Not Reported"
    elif visa == 1:
        return "Business"
    elif visa == 2:
        return "Pleasure"
    elif visa == 3:
        return "Student"
    else:
        return "Unknown"
    

def read_country_data(spark, country_input_path, country_temperature_input_path, output_path):
    """
    Reads the country and country temperature files, then combines the data and creates a country table.
    :param spark: Spark session: 
    :param country_input_path:  Path and file name of country file data: str
    :param country_temperature_input_path:  Path and file name of containing temperatures per country: str
    :param output_path: Output path for Parquet files: str
    :return: None
    """
    print(f"Reading country file {country_input_path}...")
    df_i94_countries = spark.read.load(country_input_path, format="csv", sep=",", inferSchema="true", header="true")
    print(f"number of rows read: {num_of_rows_check(spark, df_i94_countries)} ")
    df_i94_countries = df_i94_countries.withColumnRenamed("Code", "Country_Code")
    df_i94_countries.createOrReplaceTempView("countries_view")
    
    print(f"Reading Country temperature file {country_temperature_input_path}...")
    df_country_temperatures =spark.read.load("GlobalLandTemperaturesByCountry.csv", format="csv", sep=",", inferSchema="true", header="true")
    print(f"number of rows read: {num_of_rows_check(spark, df_country_temperatures)} ")
    df_country_temperatures = df_country_temperatures.select("*").where((df_country_temperatures.dt > "1999-12-31"))
    df_country_temperatures = df_country_temperatures.filter(df_country_temperatures.AverageTemperature.isNotNull())
    df_country_temperatures = df_country_temperatures.withColumn("rank", F.dense_rank().over(Window.partitionBy("Country").orderBy(F.desc("dt"))))
    df_country_temperatures = df_country_temperatures.filter(df_country_temperatures["rank"] == 1).orderBy("Country")
    df_country_temperatures.createOrReplaceTempView("country_temperatures_view")
    
    print("Creating country table...")
    df_countries = spark.sql("select distinct Country_code, cv.Country as Country, date(dt) as LastMeasuredDate, AverageTemperature, AverageTemperatureUncertainty \
    from countries_view cv \
    left outer join country_temperatures_view ct on upper(cv.Country)=upper(ct.Country)")
    df_countries = df_countries.select(['Country_code', 'Country', 'LastMeasuredDate', 'AverageTemperature', 'AverageTemperatureUncertainty'])
    output_folder = output_path + '/parquet/Country_table'
    print(f"Writing joined country table in folder {output_folder}...")
    df_countries.write.mode("overwrite").parquet(f"{output_folder}")
    
    
def read_port_data(spark, ports_input_path):
    """
    Reads the i94 port file and creates a SQL temporary view
    :param spark: Spark session
    :param ports_input_path:  Path and file name of the i94 ports file: str
    :return: temporary view: object of type 'pyspark.sql.dataframe.DataFrame'
    """
        
    print(f"Reading i94 ports file {ports_input_path}...")
    df_i94_ports = spark.read.load(ports_input_path, format="csv", sep=",", inferSchema="true", header="true")
    print(f"number of rows read: {num_of_rows_check(spark, df_i94_ports)} ")
    
    df_i94_ports = df_i94_ports.withColumnRenamed("Code", "Port_Code").withColumnRenamed("City", "Port_City").withColumnRenamed("State", "State_Code")
    df_i94_ports.createOrReplaceTempView("ports_view")
    
    return df_i94_ports


def read_demographics_data(spark, port_df_view, demo_input_path, output_path):
    """
    Reads the city demographics file 
    :param spark: Spark session
    :param port_df_view: temporary port view: object of type 'pyspark.sql.dataframe.DataFrame'
    :param demo_input_path:  Path and file name of the city demographics file: str
    :param output_path: Output path for Parquet files: str
    :return: None
    """
    print(f"Reading demographics file {demo_input_path}...")
    df_demographics =spark.read.load("us-cities-demographics.csv", format="csv", sep=";", inferSchema="true", header="true")
    print(f"number of rows read: {num_of_rows_check(spark, df_demographics)} ")
    df_demographics=df_demographics.withColumnRenamed("State Code", "State_Code").withColumnRenamed("Median Age", "Median_Age").withColumnRenamed("Male Population", \
     "Male_Population").withColumnRenamed("Female Population", "Female_Population").withColumnRenamed("Total Population", \
     "Total_Population").withColumnRenamed("Number of Veterans", \
     "Number_of_Veterans").withColumnRenamed("Foreign-born", "Foreign_born").withColumnRenamed("Average Household Size", "Avg_Household_Size")
    
    df_demographics.createOrReplaceTempView("demo_view")
    df_demographics = spark.sql("select distinct * from ports_view p  join demo_view d ON UPPER(p.Port_City) = UPPER(d.City) AND UPPER(p.State_Code) = UPPER(d.State_Code)")
    
    df_demographics=df_demographics.select(['Port_Code', 'Port_City', 'p.State_Code', 'State', 'Median_Age', 'Male_Population', 'Female_Population', 'Total_Population', \
     'Number_of_Veterans', 'Foreign_born', 'Avg_Household_Size', 'race', 'Count'])
    
    output_folder = output_path + '/parquet/Demographics_table'
    print(f"Writing joined demographic and i94_ports files to parquet files in folder {output_folder}...")
    df_demographics.write.mode("overwrite").parquet(f"{output_folder}")
        
    
def read_airport_data(spark, port_df_view, airports_input_path, output_path):
    """
    Reads airport data file and creates a airport table by joining to the I94 port data 
    :param spark: Spark session
    :param port_df_view: temporary port view: object of type 'pyspark.sql.dataframe.DataFrame'
    :param airports_input_path:  Path and file name of the airport file: str
    :param output_path: Output path for Parquet files: str
    :return: None
    """
    print(f"Reading airport file {airports_input_path}...")
    df_airports=spark.read.load(airports_input_path, format="csv", sep=",", inferSchema="true", header="true")
    print(f"number of rows read: {num_of_rows_check(spark, df_airports)} ")
    
    #Extract the longitude and latitude from the coordinates column
    coor_long = F.split(df_airports.coordinates, ",")
    df_airports = df_airports.withColumn("longitude", coor_long.getItem(0))
    df_airports = df_airports.withColumn("latitude", coor_long.getItem(1))
    
    df_airports.createOrReplaceTempView("airport_view")
    df_airports=spark.sql("select distinct ident, type as airport_type, name, elevation_ft, int(ceil(elevation_ft*0.3048)) as elevation_metres, \
    continent, iso_country, iso_region, municipality, gps_code, iata_code, local_code, longitude,  \
    latitude from airport_view where iso_country='US' ")
    
    df_airports= spark.sql("select Port_Code, Port_City, State_Code, name, type, ifnull(elevation_ft, 0) as elevation_feet, int(ceil(ifnull(elevation_ft, 0)*0.3048)) as elevation_metres, \
    continent, iso_country, iso_region, municipality, gps_code, iata_code, float(longitude), float(latitude) \
    from ports_view  join airport_view on Port_Code=iata_code and iso_region=('US-' || trim(State_Code)) where iso_country='US' and type<>'closed'")
    df_airports=df_airports.select(['Port_Code', 'Port_City', 'State_Code', 'name', 'type', 'elevation_feet', 'elevation_metres', 'continent', 'iso_country', 'iso_region', 'municipality', \
    'gps_code', 'iata_code', 'longitude', 'latitude'])
    output_folder = output_path + '/parquet/airports_table'
    print(f"Writing joined airport-codes and i94_ports files to parquet files in folder {output_folder}...")
    df_airports.write.mode("overwrite").partitionBy("State_Code") \
           .parquet(f"{output_folder}")
    
    
def read_immigration_data(spark, input_path, output_path):
    """
    Read i94 immigration data into dataframe.
    Data is then cleaned and then written to Parquet files
    Partitioned by year and month.
    :param spark: Spark session
    :param input_path: Input path to Parquet/SAS data: str
    :param output_path: Output path for Parquet files: str
    :return: None
    """
    spark.udf.register("convert_mode", lambda x: convert_i94mode(x))
    spark.udf.register("convert_visatype", lambda x: convert_visatype(x))
    spark.udf.register("datetime_from_sas", lambda x: convert_datetime(x), T.DateType())
    
    print(f"Reading immigration file {input_path}...")
    df_immigrations = spark.read.format('com.github.saurfang.sas.spark').load(input_path)
    print(f"number of rows read: {num_of_rows_check(spark, df_immigrations)} ")
    df_immigrations.createOrReplaceTempView("immigrations_view")
    df_immigrations = spark.sql("Select int(cicid) as immigration_id, int(i94yr) as Year, int(i94mon) as Month, int(i94cit) as country_of_citizenship, \
    i94port as port_code, int(biryear) as birth_year, airline as arrival_airline, fltno as arrival_flightnumber, convert_visatype(i94visa) as visa_type,\
     convert_mode(i94mode) as mode_of_travel, \
    datetime_from_sas(arrdate) as arrival_date, datetime_from_sas(depdate) as departure_date from immigrations_view where int(cicid)=5748881")
    
    # Project final data set
    df_immigrations = df_immigrations.select(
            ['immigration_id', 'Year', 'Month', 'country_of_citizenship', 'port_code', 'mode_of_travel', 'visa_type',  
              'birth_year', 'arrival_airline', 'arrival_flightnumber', 'arrival_date',  'departure_date'])
             

    # Write data in Apache Parquet format partitioned by year and month
    output_folder = output_path + '/parquet/immigrations_table'
    print(f"Writing immigration table to parquet files in folder {output_folder}...")
    df_immigrations.write.mode("append").partitionBy("Year", "Month") \
           .parquet(f"{output_folder}")
    
    print("Creating time table...")
    df_time_table=spark.sql("select distinct datetime_from_sas(arrdate) as sas_date, year(datetime_from_sas(arrdate)) as year, \
    month(datetime_from_sas(arrdate)) as month, day(datetime_from_sas(arrdate)) as day, weekofyear(datetime_from_sas(arrdate)) as weekofyear, \
    quarter(datetime_from_sas(arrdate)) as quarter, dayofweek(datetime_from_sas(arrdate)) as dayofweek from immigrations_view \
    where arrdate is not NULL union \
    select distinct datetime_from_sas(depdate) as sas_date, year(datetime_from_sas(depdate)) as year, \
    month(datetime_from_sas(depdate)) as month, day(datetime_from_sas(depdate)) as day, weekofyear(datetime_from_sas(depdate)) as weekofyear, \
    quarter(datetime_from_sas(depdate)) as quarter, dayofweek(datetime_from_sas(depdate)) as dayofweek from immigrations_view \
    where depdate is not NULL")
    df_time_table = df_time_table.select(['sas_date', 'year', 'month', 'weekofyear', 'quarter', 'dayofweek'])
    
    output_folder = output_path + '/parquet/time_table'
    print(f"Writing time table to parquet files in folder {output_folder}...")
    df_time_table.write.mode("append").partitionBy("year", "month") \
           .parquet(f"{output_folder}")
    
def read_city_temperatures(spark, port_df_view, city_temperature_input_path, output_path):
    """
    Reads city temperatures and joins to the I94 port data 
    Data is then cleaned and written to Parquet files
    Partitioned by year and month.
    :param spark: Spark session
    :param port_df_view: temporary port view: object of type 'pyspark.sql.dataframe.DataFrame'
    :param city_tempeature_input_path: Input path to city temperatues file: str
    :param output_path: Output path for Parquet files: str
    :return: None
    """
    print(f"Reading city temperatures file {city_temperature_input_path}...")
    df =spark.read.load("../../data2/GlobalLandTemperaturesByCity.csv", format="csv", sep=",", inferSchema="true", header="true")
    print(f"number of rows read: {num_of_rows_check(spark, df)} ")
    
    df2 = df.select("*").where((df.dt > "1999-12-31"))
    df2 = df2.filter(df2.Country == "United States") \
        .withColumn("rank", F.dense_rank().over(Window.partitionBy("City").orderBy(F.desc("dt"))))
    df_city_temperatures = df2.filter(df2["rank"] == 1).orderBy("City")
    df_city_temperatures.createOrReplaceTempView("city_temperatures_view")
    df_city_temperatures = spark.sql("select distinct Port_Code, Port_City,  date(dt) as LastMeasuredDate, AverageTemperature, AverageTemperatureUncertainty \
     from Ports_view \
     join city_temperatures_view on upper(Port_city)=upper(City) where Port_City is not null ")
    
    #print(f"number of rows in view: {num_of_rows_check(spark, df_city_temperatures)} ")
    df_city_temperatures = df_city_temperatures.select(['Port_Code', 'Port_City', 'LastMeasuredDate', 'AverageTemperature', 'AverageTemperatureUncertainty'])
    output_folder = output_path + '/parquet/city_temperatures'
    print(f"Writing time table to parquet files in folder {output_folder}...")
    df_city_temperatures.write.mode("append").parquet(f"{output_folder}")
    
    
def num_of_rows_check(spark, input_df):
    """
    Count the number of rows read into a dataframe
    :param spark: Spark session
    :param input_df: input dataframe: object of class 'pyspark.sql.dataframe.DataFrame'
    :return: Number of rows: int
    """
    
    input_df.createOrReplaceTempView("input_view")
    number_of_rows = spark.sql("""
        SELECT  COUNT(*)
        FROM input_view
    """)
   
    return number_of_rows.collect()[0][0]
    
   
def main():
    spark = create_spark_session()
    
    read_country_data(spark, COUNTRY_INPUT_PATH, COUNTRY_TEMPERATURE_INPUT_PATH, OUTPUT_PATH)
    df_i94_ports_view = read_port_data(spark, PORTS_INPUT_PATH)
    read_demographics_data(spark, df_i94_ports_view, DEMO_INPUT_PATH, OUTPUT_PATH)
    
    read_airport_data(spark, df_i94_ports_view, AIRPORTS_INPUT_PATH, OUTPUT_PATH)
    read_immigration_data(spark, I94_INPUT_PATH, OUTPUT_PATH)
    read_city_temperatures(spark, df_i94_ports_view, CITY_TEMPERATURE_INPUT_PATH, OUTPUT_PATH)
    
    
if __name__ == "__main__":
    main()