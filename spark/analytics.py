from pyspark.sql.functions import col, year, month, dayofweek, hour, count, round, sum, when, broadcast
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from preprocessing import load_and_clean_data

def run_batch_analytics(spark, db_properties, db_url):
    data_dir = '/data'
    datasets = load_and_clean_data(spark, data_dir)
    
    df_crime = datasets['crime']
    df_stations = datasets['stations']
    df_arrests = datasets['arrests']
    df_violence = datasets['violence']
    df_offenders = datasets['offenders']
    
    # ---------------------------------------------------------
    # 7.1 Crime Trend Analysis
    # ---------------------------------------------------------
    print("Computing Crime Trend Analysis...")
    trends = df_crime.filter(col("DATE_PARSED").isNotNull()) \
        .withColumn("year", year("DATE_PARSED")) \
        .withColumn("month", month("DATE_PARSED")) \
        .withColumn("day_of_week", dayofweek("DATE_PARSED")) \
        .withColumn("hour_of_day", hour("DATE_PARSED")) \
        .groupBy("year", "month", "day_of_week", "hour_of_day") \
        .agg(count("*").alias("crime_count"))
    
    trends.write.jdbc(url=db_url, table="crime_trends", mode="overwrite", properties=db_properties)
    
    # ---------------------------------------------------------
    # 7.2 Arrest Rate Analysis
    # ---------------------------------------------------------
    print("Computing Arrest Rate Analysis...")
    df_crime_arrests = df_crime.join(df_arrests, "CASE NUMBER", "left_outer")
    
    rates_by_group = df_crime_arrests.fillna({'RACE': 'UNKNOWN'}).groupBy("PRIMARY TYPE", "DISTRICT", "RACE").agg(
        count("*").alias("total_crimes"),
        sum(when(col("ARREST DATE").isNotNull(), 1).otherwise(0)).alias("total_arrests")
    ).withColumn("arrest_rate", round(col("total_arrests") / col("total_crimes"), 4)) \
    .withColumnRenamed("PRIMARY TYPE", "primary_type") \
    .withColumnRenamed("DISTRICT", "district") \
    .withColumnRenamed("RACE", "race")
    
    rates_by_group.write.jdbc(url=db_url, table="arrest_rates", mode="overwrite", properties=db_properties)
    
    top_10_crimes = df_crime_arrests.groupBy("PRIMARY TYPE").agg(
        count("*").alias("total_crimes"),
        sum(when(col("ARREST DATE").isNotNull(), 1).otherwise(0)).alias("total_arrests")
    ).withColumn("arrest_rate", round(col("total_arrests") / col("total_crimes"), 4)) \
    .orderBy(col("arrest_rate").desc()).limit(10) \
    .withColumnRenamed("PRIMARY TYPE", "primary_type")
    
    top_10_crimes.write.jdbc(url=db_url, table="top_10_arrest_rates", mode="overwrite", properties=db_properties)
    
    # ---------------------------------------------------------
    # 7.3 Violence and Gunshot Analysis
    # ---------------------------------------------------------
    print("Computing Violence Analysis...")
    violence_stats = df_violence.groupBy("MONTH", "DISTRICT").agg(
        sum(when(col("HOMICIDE VICTIM *") == 'Y', 1).otherwise(0)).alias("homicides"),
        sum(when(col("HOMICIDE VICTIM *") == 'N', 1).otherwise(0)).alias("non_fatal_shootings"),
        sum(when(col("GUNSHOT INJURY I") == 'Y', 1).otherwise(0)).alias("gunshot_injuries"),
        count("*").alias("total_incidents")
    ).withColumn("gunshot_proportion", round(col("gunshot_injuries") / col("total_incidents"), 4)) \
    .withColumnRenamed("MONTH", "month") \
    .withColumnRenamed("DISTRICT", "district")
    
    violence_stats.write.jdbc(url=db_url, table="violence_analysis", mode="overwrite", properties=db_properties)
    
    top_community_areas = df_violence.groupBy("COMMUNITY AREA").agg(count("*").alias("violence_count")) \
        .orderBy(col("violence_count").desc()).limit(10) \
        .withColumnRenamed("COMMUNITY AREA", "community_area")
    
    top_community_areas.write.jdbc(url=db_url, table="top_violent_communities", mode="overwrite", properties=db_properties)

    # ---------------------------------------------------------
    # 7.4 Sex Offender Proximity Analysis
    # ---------------------------------------------------------
    print("Computing Sex Offender Proximity Analysis...")
    block_districts = df_crime.select("BLOCK", "DISTRICT").dropDuplicates(["BLOCK"])
    offenders_with_district = df_offenders.join(block_districts, "BLOCK", "left_outer")
    offenders_flagged = offenders_with_district.withColumn("priority_flag", when(col("VICTIM MINOR") == "Y", True).otherwise(False))
    
    offender_density = offenders_flagged.groupBy("DISTRICT").agg(count("*").alias("offender_count"))
        
    proximity_analysis = offender_density.join(df_stations, "DISTRICT", "inner") \
        .select("DISTRICT", "DISTRICT NAME", "ADDRESS", "offender_count") \
        .orderBy(col("offender_count").desc()) \
        .withColumnRenamed("DISTRICT", "district") \
        .withColumnRenamed("DISTRICT NAME", "district_name") \
        .withColumnRenamed("ADDRESS", "station_address")
    
    proximity_analysis.write.jdbc(url=db_url, table="sex_offender_density", mode="overwrite", properties=db_properties)

    # ---------------------------------------------------------
    # 7.5 Geospatial Hotspot Detection
    # ---------------------------------------------------------
    print("Running K-Means Hotspot Detection...")
    coords = df_crime.filter(col("LATITUDE").isNotNull() & col("LONGITUDE").isNotNull())
    assembler = VectorAssembler(inputCols=["LATITUDE", "LONGITUDE"], outputCol="features")
    dataset = assembler.transform(coords)
    kmeans = KMeans(k=10, seed=1)
    model = kmeans.fit(dataset)
    predictions = model.transform(dataset)
    
    hotspots = predictions.selectExpr("prediction as cluster_id", "LATITUDE as latitude", "LONGITUDE as longitude").limit(100)
    hotspots.write.jdbc(url=db_url, table="hotspots", mode="overwrite", properties=db_properties)

    # ---------------------------------------------------------
    # 7.6 Cross-Dataset Correlation
    # ---------------------------------------------------------
    print("Computing Cross-Dataset Correlations...")
    arrest_rates_dist = df_crime_arrests.groupBy("DISTRICT").agg(
        count("*").alias("total_crimes"),
        sum(when(col("ARREST DATE").isNotNull(), 1).otherwise(0)).alias("total_arrests")
    ).withColumn("arrest_rate", col("total_arrests") / col("total_crimes"))
    
    violence_rates_dist = df_violence.groupBy("DISTRICT").agg(count("*").alias("violence_count"))
    
    corr1_df = arrest_rates_dist.join(violence_rates_dist, "DISTRICT", "inner")
    corr1 = corr1_df.corr("arrest_rate", "violence_count")
    
    crime_counts = df_crime.groupBy("DISTRICT").agg(count("*").alias("crime_count"))
    corr2_df = offender_density.join(crime_counts, "DISTRICT", "inner")
    corr2 = corr2_df.corr("offender_count", "crime_count")
    
    spark.createDataFrame([
        ("Violence vs Arrest Rate (by district)", float(corr1) if corr1 else 0.0),
        ("Offender Density vs Crime Count (by district)", float(corr2) if corr2 else 0.0)
    ], ["correlation_type", "correlation_value"]) \
    .write.jdbc(url=db_url, table="correlations", mode="overwrite", properties=db_properties)

    print("Batch analytics complete.")

if __name__ == "__main__":
    from pyspark.sql import SparkSession
    spark = SparkSession.builder \
        .appName("CrimeAnalyticsBatch") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.5.4") \
        .getOrCreate()
    db_properties = {
        "user": "admin",
        "password": "password",
        "driver": "org.postgresql.Driver"
    }
    db_url = "jdbc:postgresql://postgres:5432/crime_db"
    run_batch_analytics(spark, db_properties, db_url)
