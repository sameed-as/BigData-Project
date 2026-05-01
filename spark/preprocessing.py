from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType
from pyspark.sql.functions import col, to_timestamp

# Schema definitions

crime_schema = StructType([
    StructField("ID", IntegerType(), True),
    StructField("CASE NUMBER", StringType(), True),
    StructField("DATE", StringType(), True),
    StructField("BLOCK", StringType(), True),
    StructField("IUCR", StringType(), True),
    StructField("PRIMARY TYPE", StringType(), True),
    StructField("DESCRIPTION", StringType(), True),
    StructField("LOCATION DESCRIPTION", StringType(), True),
    StructField("ARREST", BooleanType(), True),
    StructField("DOMESTIC", BooleanType(), True),
    StructField("BEAT", StringType(), True),
    StructField("DISTRICT", StringType(), True),
    StructField("WARD", StringType(), True),
    StructField("COMMUNITY AREA", StringType(), True),
    StructField("FBI CODE", StringType(), True),
    StructField("X COORDINATE", DoubleType(), True),
    StructField("Y COORDINATE", DoubleType(), True),
    StructField("YEAR", IntegerType(), True),
    StructField("UPDATED ON", StringType(), True),
    StructField("LATITUDE", DoubleType(), True),
    StructField("LONGITUDE", DoubleType(), True),
    StructField("LOCATION", StringType(), True)
])

police_stations_schema = StructType([
    StructField("DISTRICT", StringType(), True),
    StructField("DISTRICT NAME", StringType(), True),
    StructField("ADDRESS", StringType(), True),
    StructField("CITY", StringType(), True),
    StructField("STATE", StringType(), True),
    StructField("ZIP", StringType(), True),
    StructField("WEBSITE", StringType(), True),
    StructField("PHONE", StringType(), True),
    StructField("FAX", StringType(), True),
    StructField("TTY", StringType(), True),
    StructField("X COORDINATE", DoubleType(), True),
    StructField("Y COORDINATE", DoubleType(), True),
    StructField("LATITUDE", DoubleType(), True),
    StructField("LONGITUDE", DoubleType(), True),
    StructField("LOCATION", StringType(), True)
])

arrests_schema = StructType([
    StructField("CB NO", StringType(), True),
    StructField("CASE NUMBER", StringType(), True),
    StructField("ARREST DATE", StringType(), True),
    StructField("RACE", StringType(), True),
    StructField("CHARGE 1 STATUTE", StringType(), True),
    StructField("CHARGE 1 DESCRIPTION", StringType(), True),
    StructField("CHARGE 1 TYPE", StringType(), True),
    StructField("CHARGE 1 CLASS", StringType(), True),
    StructField("CHARGE 2 STATUTE", StringType(), True),
    StructField("CHARGE 2 DESCRIPTION", StringType(), True),
    StructField("CHARGE 2 TYPE", StringType(), True),
    StructField("CHARGE 2 CLASS", StringType(), True),
    StructField("CHARGE 3 STATUTE", StringType(), True),
    StructField("CHARGE 3 DESCRIPTION", StringType(), True),
    StructField("CHARGE 3 TYPE", StringType(), True),
    StructField("CHARGE 3 CLASS", StringType(), True),
    StructField("CHARGE 4 STATUTE", StringType(), True),
    StructField("CHARGE 4 DESCRIPTION", StringType(), True),
    StructField("CHARGE 4 TYPE", StringType(), True),
    StructField("CHARGE 4 CLASS", StringType(), True)
])

violence_schema = StructType([
    StructField("CASE NUMBER", StringType(), True),
    StructField("DATE", StringType(), True),
    StructField("BLOCK", StringType(), True),
    StructField("VICTIMIZATION FBI CD", StringType(), True),
    StructField("INCIDENT FBI CD", StringType(), True),
    StructField("HOMICIDE VICTIM *", StringType(), True),
    StructField("GUNSHOT INJURY I", StringType(), True),
    StructField("AGE", IntegerType(), True),
    StructField("SEX", StringType(), True),
    StructField("RACE", StringType(), True),
    StructField("MONTH", IntegerType(), True),
    StructField("DAY OF WEEK", IntegerType(), True),
    StructField("ZIP CODE", StringType(), True),
    StructField("WARD", StringType(), True),
    StructField("COMMUNITY AREA", StringType(), True),
    StructField("DISTRICT", StringType(), True),
    StructField("BEAT", StringType(), True)
])

sex_offenders_schema = StructType([
    StructField("LAST", StringType(), True),
    StructField("FIRST", StringType(), True),
    StructField("BLOCK", StringType(), True),
    StructField("GENDER", StringType(), True),
    StructField("RACE", StringType(), True),
    StructField("BIRTH DATE", StringType(), True),
    StructField("HEIGHT", StringType(), True),
    StructField("WEIGHT", StringType(), True),
    StructField("VICTIM MINOR", StringType(), True)
])

def load_and_clean_data(spark, data_dir):
    """
    Loads all datasets using explicit schemas, handles nulls and casting.
    Returns a dictionary of cleaned dataframes.
    """
    # Crime
    df_crime = spark.read.csv(f"{data_dir}/crime.csv", schema=crime_schema, header=True)
    df_crime = df_crime.withColumn("DATE_PARSED", to_timestamp(col("DATE"), "MM/dd/yyyy hh:mm:ss a"))
    
    # Police Stations
    df_stations = spark.read.csv(f"{data_dir}/police_stations.csv", schema=police_stations_schema, header=True)
    
    # Arrests
    df_arrests = spark.read.csv(f"{data_dir}/arrests.csv", schema=arrests_schema, header=True)
    
    # Violence
    df_violence = spark.read.csv(f"{data_dir}/violence.csv", schema=violence_schema, header=True)
    
    # Sex Offenders
    df_offenders = spark.read.csv(f"{data_dir}/sex_offenders.csv", schema=sex_offenders_schema, header=True)
    
    return {
        'crime': df_crime,
        'stations': df_stations,
        'arrests': df_arrests,
        'violence': df_violence,
        'offenders': df_offenders
    }
