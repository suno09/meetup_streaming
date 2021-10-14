from pyspark.sql import SparkSession, DataFrameWriter
from pyspark.sql.functions import from_json, col, lit, count
from pyspark.sql.types import StructType, StringType, StructField, ArrayType

print("Stream Processing Application Started ...")

# Code Block 1 Starts Here
kafka_topic_name = "meetuprsvptopic"
kafka_bootstrap_servers = "localhost:9092"

mysql_host_name = "localhost"
mysql_port_no = "3306"
mysql_user_name = "root"
mysql_password = "0000"
mysql_database_name = "meetup_rsvp_db"
mysql_driver_class = "com.mysql.jdbc.Driver"
mysql_table_name = "meetup_rsvp_message_agg_detail_tbl"
mysql_jdbc_url = "jdbc:mysql://" + mysql_host_name + ":" + mysql_port_no + "/" + mysql_database_name + "?createDatabaseIfNotExist=true"

mongodb_host_name = "localhost"
mongodb_port_no = "27017"
mongodb_user_name = "root"
mongodb_password = "0000"
mongodb_database_name = "meetup_rsvp_db"
mongodb_collection_name = "meetup_rsvp_message_detail_tbl"
mongodb_uri = "mongodb://localhost:27017/meetup_rsvp_db"
# Code Block 1 Ends Here

# Code Block 2 Starts Here
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('Stream Processing Application') \
    .config('spark.jars.packages',
            'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,'
            'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
    .getOrCreate()
# .config('spark.mongodb.input.uri', mongodb_uri)\
# .config('spark.mongodb.output.uri', mongodb_uri)\
# .config('spark.jars.packages', "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1")\

sc = spark.sparkContext
sc.setLogLevel("ERROR")
# Code Block 2 Ends Here

# Code Block 3 Starts Here
meetup_rsvp_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic_name) \
    .option("startingOffsets", "latest") \
    .load()

print("Printing Schema of transaction_detail_df: ")
meetup_rsvp_df.printSchema()
# Code Block 3 Ends Here

# Code Block 4 Starts Here
# Define a schema for the message_detail data
meetup_rsvp_message_schema = StructType([
    StructField("venue", StructType([
        StructField("venue_name", StringType()),
        StructField("lon", StringType()),
        StructField("lat", StringType()),
        StructField("venue_id", StringType())
    ])),
    StructField("visibility", StringType()),
    StructField("response", StringType()),
    StructField("guests", StringType()),
    StructField("member", StructType([
        StructField("member_id", StringType()),
        StructField("photo", StringType()),
        StructField("member_name", StringType())
    ])),
    StructField("rsvp_id", StringType()),
    StructField("mtime", StringType()),
    StructField("event", StructType([
        StructField("event_name", StringType()),
        StructField("event_id", StringType()),
        StructField("time", StringType()),
        StructField("event_url", StringType())
    ])),
    StructField("group", StructType([
        StructField("group_topics", ArrayType(StructType([
            StructField("urlkey", StringType()),
            StructField("topic_name", StringType())
        ]), True)),
        StructField("group_city", StringType()),
        StructField("group_country", StringType()),
        StructField("group_id", StringType()),
        StructField("group_name", StringType()),
        StructField("group_lon", StringType()),
        StructField("group_urlname", StringType()),
        StructField("group_state", StringType()),
        StructField("group_lat", StringType())
    ]))
])

meetup_rsvp_df_1 = meetup_rsvp_df.selectExpr("CAST(value AS STRING)",
                                             "CAST(timestamp AS TIMESTAMP)")

meetup_rsvp_df_2 = meetup_rsvp_df_1.select(
    from_json(col("value"), meetup_rsvp_message_schema).name("message_detail"),
    col("timestamp"))

meetup_rsvp_df_3 = meetup_rsvp_df_2.select("message_detail.*", "timestamp")

meetup_rsvp_df_4 = meetup_rsvp_df_3.select(col("group.group_name"),
                                           col("group.group_country"),
                                           col("group.group_state"),
                                           col("group.group_city"),
                                           col("group.group_lat"),
                                           col("group.group_lon"),
                                           col("group.group_id"),
                                           col("group.group_topics"),
                                           col("member.member_name"),
                                           col("response"),
                                           col("guests"),
                                           col("venue.venue_name"),
                                           col("venue.lon"), col("venue.lat"),
                                           col("venue.venue_id"),
                                           col("visibility"),
                                           col("member.member_id"),
                                           col("member.photo"),
                                           col("event.event_name"),
                                           col("event.event_id"),
                                           col("event.time"),
                                           col("event.event_url")
                                           )

print("Printing Schema of meetup_rsvp_df_4: ")
meetup_rsvp_df_4.printSchema()
# Code Block 4 Ends Here

# Code Block 5 Starts Here
# Writing Meetup RSVP DataFrame into MongoDB Collection Starts Here
spark_mongodb_output_uri = "mongodb://" + mongodb_user_name + ":" + mongodb_password + "@" + mongodb_host_name + ":" + mongodb_port_no
# + "/" + mongodb_database_name + "." + mongodb_collection_name
print("Printing spark_mongodb_output_uri: " + spark_mongodb_output_uri)


def write_to_mongo_data(batch_df, batch_id):
    batch_df_ = batch_df.withColumn("batch_id", lit(batch_id))
    # Transform batchDF and write it to sink / target / persistent storage
    # Write data from spark dataframe to database
    batch_df_.write \
        .format('mongo') \
        .mode("append") \
        .option('uri', spark_mongodb_output_uri) \
        .option('database', mongodb_database_name) \
        .option('collection', mongodb_collection_name) \
        .save()


meetup_rsvp_df_4.writeStream \
    .trigger(processingTime="20 seconds") \
    .outputMode("update") \
    .foreachBatch(
        lambda batch_df, batch_id: write_to_mongo_data(batch_df, batch_id)) \
    .start()

# Writing Aggregated Meetup RSVP DataFrame into MySQL Database Table Ends Here
# Code Block 5 Ends Here

# Code Block 6 Starts Here
# Simple aggregate - find response_count by grouping group_name,
# group_country, group_state, group_city, group_lat, group_lon, response

meetup_rsvp_df_5 = meetup_rsvp_df_4.groupBy("group_name", "group_country",
                                            "group_state", "group_city",
                                            "group_lat", "group_lon",
                                            "response").agg(
    count(col("response")).name("response_count"))

print("Printing Schema of meetup_rsvp_df_5: ")
meetup_rsvp_df_5.printSchema()
# Code Block 6 Ends Here

# Code Block 7 Starts Here
# Write final result into console for debugging purpose
trans_detail_write_stream = meetup_rsvp_df_5.writeStream \
    .trigger(processingTime="20 seconds") \
    .outputMode("update") \
    .option("truncate", "false") \
    .format("console") \
    .start()
# Code Block 7 Ends Here

# Code Block 8 Starts Here
# Writing Aggregated Meetup RSVP DataFrame into MySQL Database Table Starts Here

print("mysql_jdbc_url: " + mysql_jdbc_url)


def write_to_sql_data(batch_df, batch_id):
    batch_df_ = batch_df.withColumn("batch_id", lit(batch_id))
    # Transform batchDF and write it to sink / target / persistent storage
    # Write data from spark dataframe to database
    batch_df_.write \
        .jdbc(url=mysql_jdbc_url,
              table=mysql_table_name,
              mode="append",
              properties={'driver': 'com.mysql.cj.jdbc.Driver',
                          'user': 'root',
                          'password': '0000'})


meetup_rsvp_df_5.writeStream \
    .trigger(processingTime="20 seconds") \
    .outputMode("update") \
    .foreachBatch(
        lambda batch_df, batch_id: write_to_sql_data(batch_df, batch_id)) \
    .start()
# Writing Aggregated Meetup RSVP DataFrame into MySQL Database Table Ends Here

trans_detail_write_stream.awaitTermination()
# Code Block 8 Ends Here

print("Stream Processing Application Completed.")
