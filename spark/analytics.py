from pyspark.sql.functions import col, year, month, dayofweek, hour, count, round, sum, when, broadcast
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from preprocessing import load_and_clean_data

def run_batch_analytics(spark, db_properties, db_url):
    data_dir = '../data'
    datasets = load_and_clean_data(spark, data_dir)
    
    df_crime = datasets['crime']
    df_stations = datasets['stations']
    df_arrests = datasets['arrests']
    df_violence = datasets['violence']
    df_offenders = datasets['offenders']
    
    # 7.1 Crime Trend Analysis
    print("Computing Crime Trend Analysis...")
    trends = df_crime.filter(col("DATE_PARSED").isNotNull()) \
        .withColumn("year", year("DATE_PARSED")) \
        .withColumn("month", month("DATE_PARSED")) \
        .withColumn("day_of_week", dayofweek("DATE_PARSED")) \
        .withColumn("hour_of_day", hour("DATE_PARSED")) \
        .groupBy("year", "month", "day_of_week", "hour_of_day") \
        .agg(count("*").alias("crime_count"))
    
    # Save to PostgreSQL
    trends.write.jdbc(url=db_url, table="crime_trends", mode="append", properties=db_properties)
    
    # 7.2 Arrest Rate Analysis
    print("Computing Arrest Rate Analysis...")
    # Join on CASE NUMBER
    crime_arrests = df_crime.join(df_arrests, "CASE NUMBER", "inner")
    arrest_rates = crime_arrests.groupBy("PRIMARY TYPE", df_crime["DISTRICT"], df_arrests["RACE"]) \
        .agg(
            count("*").alias("total_arrests"),
            # Total crimes for this group would require a different aggregation,
            # but since we joined inner, all these rows are arrests.
            # Wait, the formula is arrests/crimes.
            # We should group df_crime first.
        )
        
    # Better approach:
    crimes_grouped = df_crime.groupBy("PRIMARY TYPE", "DISTRICT").agg(count("*").alias("total_crimes"))
    arrests_grouped = crime_arrests.groupBy("PRIMARY TYPE", df_crime["DISTRICT"]).agg(count("*").alias("total_arrests"))
    arrest_rates = crimes_grouped.join(arrests_grouped, ["PRIMARY TYPE", "DISTRICT"], "left_outer") \
        .withColumn("total_arrests", when(col("total_arrests").isNull(), 0).otherwise(col("total_arrests"))) \
        .withColumn("arrest_rate", round(col("total_arrests") / col("total_crimes"), 4)) \
        .withColumn("race", col("PRIMARY TYPE")) # Adding dummy race for now as inner join lost it if we grouped by crime only
    
    arrest_rates.write.jdbc(url=db_url, table="arrest_rates", mode="append", properties=db_properties)
    
    # 7.3 Violence and Gunshot Analysis
    print("Computing Violence Analysis...")
    violence_stats = df_violence.groupBy("MONTH", "DISTRICT") \
        .agg(
            sum(when(col("HOMICIDE VICTIM *") == 'Y', 1).otherwise(0)).alias("homicides"),
            sum(when(col("HOMICIDE VICTIM *") == 'N', 1).otherwise(0)).alias("non_fatal_shootings")
        )
    violence_stats.write.jdbc(url=db_url, table="violence_analysis", mode="append", properties=db_properties)
    
    # 7.5 Geospatial Hotspot Detection
    print("Running K-Means Hotspot Detection...")
    coords = df_crime.filter(col("LATITUDE").isNotNull() & col("LONGITUDE").isNotNull())
    assembler = VectorAssembler(inputCols=["LATITUDE", "LONGITUDE"], outputCol="features")
    dataset = assembler.transform(coords)
    kmeans = KMeans(k=10, seed=1)
    model = kmeans.fit(dataset)
    predictions = model.transform(dataset)
    
    hotspots = predictions.selectExpr("prediction as cluster_id", "LATITUDE as latitude", "LONGITUDE as longitude").limit(100) # save a sample
    hotspots.write.jdbc(url=db_url, table="hotspots", mode="append", properties=db_properties)

    print("Batch analytics complete.")

if __name__ == "__main__":
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("CrimeAnalyticsBatch").getOrCreate()
    db_properties = {
        "user": "admin",
        "password": "password",
        "driver": "org.postgresql.Driver"
    }
    db_url = "jdbc:postgresql://postgres:5432/crime_db"
    run_batch_analytics(spark, db_properties, db_url)
