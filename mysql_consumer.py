from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, schema_of_json, when, lit, date_format

# Créer une session Spark avec les connecteurs nécessaires

spark = SparkSession.builder \
    .appName("WeatherConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,com.datastax.spark:spark-cassandra-connector_2.12:3.1.0,mysql:mysql-connector-java:8.0.33") \
    .config("spark.cassandra.connection.host", "localhost") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

# Configuration de la connexion Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test_topic") \
    .option("failOnDataLoss", "false") \
    .load()

# Définition du schéma des données météo
schema = schema_of_json('''{
    "location": {
        "name": "", 
        "region": "", 
        "country": "", 
        "lat": 0.0, 
        "lon": 0.0, 
        "tz_id": "", 
        "localtime_epoch": 0, 
        "localtime": ""
    }, 
    "current": {
        "last_updated_epoch": 0, 
        "last_updated": "", 
        "temp_c": 0.0, 
        "temp_f": 0.0, 
        "is_day": 0, 
        "condition": {
            "text": "", 
            "icon": "", 
            "code": 0
        }, 
        "wind_mph": 0.0, 
        "wind_kph": 0.0, 
        "wind_degree": 0, 
        "wind_dir": "", 
        "pressure_mb": 0.0, 
        "pressure_in": 0.0, 
        "precip_mm": 0.0, 
        "precip_in": 0.0, 
        "humidity": 0, 
        "cloud": 0, 
        "feelslike_c": 0.0, 
        "feelslike_f": 0.0, 
        "vis_km": 0.0, 
        "vis_miles": 0.0, 
        "uv": 0.0, 
        "gust_mph": 0.0, 
        "gust_kph": 0.0
    }
}''')

# Transformation des données Kafka
weather_data = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data"))

# Extraction et transformation des données météo
weather_info = weather_data.select(
    col("data.location.name").alias("location_name"),
    col("data.location.region").alias("region"),
    col("data.location.country").alias("country"),
    col("data.location.lat").alias("latitude"),
    col("data.location.lon").alias("longitude"),
    col("data.location.localtime").alias("local_time"),
    col("data.current.temp_c").alias("temperature_c"),
    col("data.current.temp_f").alias("temperature_f"),
    col("data.current.condition.text").alias("condition_text"),
    col("data.current.condition.icon").alias("condition_icon"),
    col("data.current.humidity").alias("humidity"),
    col("data.current.wind_mph").alias("wind_mph"),
    col("data.current.wind_kph").alias("wind_kph"),
    col("data.current.pressure_mb").alias("pressure_mb"),
    col("data.current.feelslike_c").alias("feelslike_c"),
    col("data.current.feelslike_f").alias("feelslike_f"),
    col("data.current.is_day").alias("is_day"),
    col("data.current.last_updated").alias("last_updated"),
    col("data.current.precip_mm").alias("precipitation_mm")
)

# Ajout des transformations supplémentaires
transformed_weather_info = weather_info \
    .withColumn("formatted_time", date_format(col("local_time"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("day_or_night", when(col("is_day") == 1, lit("Day")).otherwise(lit("Night")))

# Fonction pour écrire dans MySQL
def write_to_mysql(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3308/weather_db") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "weather_data") \
        .option("user", "root") \
        .option("password", "mypassword") \
        .mode("append") \
        .save()


# Écriture dans MySQL
mysql_query = transformed_weather_info.writeStream \
    .foreachBatch(write_to_mysql) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoint_mysql") \
    .start()

# Attente de l'arrêt de la lecture
mysql_query.awaitTermination()
