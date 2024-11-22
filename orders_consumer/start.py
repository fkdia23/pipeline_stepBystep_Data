from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time


customers_data_file_path = "../datasets/customers.csv"

def create_spark_session():
    """Créer et configurer la session Spark"""
    return SparkSession.builder \
        .appName("OrdersConsumer") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,"
                "com.datastax.spark:spark-cassandra-connector_2.12:3.1.0,"
                "mysql:mysql-connector-java:8.0.33") \
        .config("spark.cassandra.connection.host", "localhost") \
        .config("spark.cassandra.connection.port", "9042") \
        .getOrCreate()

def create_kafka_stream(spark):
    """Configurer et créer le stream Kafka"""
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "test_topic") \
        .option("failOnDataLoss", "false") \
        .option("startingOffsets", "latest") \
        .load()

def get_orders_schema():
    """Définir le schéma des données de commande"""
    return StructType([
        StructField("order_id", StringType(), True),
        StructField("created_at", StringType(), True),  # Changé en StringType
        StructField("discount", StringType(), True),    # Changé en StringType
        StructField("product_id", StringType(), True),
        StructField("quantity", StringType(), True),    # Changé en StringType
        StructField("subtotal", StringType(), True),    # Changé en StringType
        StructField("tax", StringType(), True),         # Changé en StringType
        StructField("total", StringType(), True),       # Changé en StringType
        StructField("customer_id", StringType(), True)
    ])

def process_stream(kafka_df, schema):
    """Transformer les données du stream"""
    # Parse JSON data
    parsed_df = kafka_df.selectExpr("CAST(value AS STRING)", "timestamp") \
        .select(from_json(col("value"), schema).alias("data") , "timestamp")

    # Sélectionner et transformer les colonnes avec les conversions appropriées
    transformed_df = parsed_df.select(
        col("data.order_id"),
        to_timestamp(col("data.created_at")).alias("created_at"),
        col("data.product_id"),
        col("data.quantity").cast(IntegerType()).alias("quantity"),
        col("data.subtotal").cast(DoubleType()).alias("subtotal"),
        col("data.discount").cast(DoubleType()).alias("discount"),
        col("data.total").cast(DoubleType()).alias("total"),
        col("data.tax").cast(DoubleType()).alias("tax"),
        col("data.customer_id")
    )

    # Ajouter un watermark pour les données tardives
    return transformed_df.withWatermark("created_at", "1 hour")

def write_to_cassandra(transformed_df):
    """Écrire les données dans Cassandra"""
    return transformed_df.writeStream \
        .outputMode("append") \
        .format("org.apache.spark.sql.cassandra") \
        .option("checkpointLocation", "/tmp/checkpoint/orders") \
        .option("keyspace", "sales_ks") \
        .option("table", "orders_tbl") \
        .trigger(processingTime="1 minute") \
        .start()

def write_to_mysql(batch_df, batch_id):

    current_df_final = batch_df \
    .withColumn("processed_at", current_timestamp()) \
    .withColumn("batch_id", lit(batch_id))

    current_df_final.write \
        .format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3308/sales_db") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "total_sales_by_source_state") \
        .option("user", "root") \
        .option("password", "mypassword") \
        .mode("append") \
        .save()

def main():
    try:
        # Initialisation
        spark = create_spark_session()

        # Lecture des données clients
        customers_df = spark.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv(customers_data_file_path)
        
        spark.sparkContext.setLogLevel("WARN")
        
        # Création du stream
        kafka_stream = create_kafka_stream(spark)
        schema = get_orders_schema()
        
        # Traitement du stream
        transformed_stream = process_stream(kafka_stream, schema)

            # Jointure avec les données clients
        orders_enriched = transformed_stream \
        .join(customers_df, "customer_id", "inner")
        
        # Agrégation par source et état
        orders_aggregated = orders_enriched \
        .groupBy("source", "state") \
        .agg(sum("total").alias("total_sum_amount"))

        # Écriture dans Cassandra
        query = write_to_cassandra(transformed_stream)


        mysql_query = orders_aggregated.writeStream \
            .outputMode("update") \
            .foreachBatch(write_to_mysql) \
            .option("checkpointLocation", "/tmp/checkpoint_mysql/state") \
            .start()
        
        # Attente de l'arrêt du stream
        query.awaitTermination()
        mysql_query.awaitTermination()
        
    except Exception as e:
        print(f"Une erreur est survenue: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()