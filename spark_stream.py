import logging

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
from cassandra.auth import PlainTextAuthProvider
import logging
import traceback


from pyspark.sql import SparkSession
# spark = SparkSession.builder.getOrCreate()
# print(spark.sparkContext._jvm.scala.util.Properties.versionString())

def check_spark_scala_version():
    """
    Check the Scala version used by Spark.
    """
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        scala_version = spark.sparkContext._jvm.scala.util.Properties.versionString()
        spark_version = spark.version
        print(f"Spark version: {spark_version}")
        print(f"Scala version: {scala_version}")
        spark.stop()
    except Exception as e:
        print(f"Error checking Spark/Scala version: {e}")

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")


def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.created_users (
        id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        post_code TEXT,
        email TEXT,
        username TEXT,
        registered_date TEXT,
        phone TEXT,
        picture TEXT);
    """)

    print("Table created successfully!")


def insert_data(session, **kwargs):
    print("inserting data...")

    user_id = kwargs.get('id')
    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    postcode = kwargs.get('post_code')
    email = kwargs.get('email')
    username = kwargs.get('username')
    dob = kwargs.get('dob')
    registered_date = kwargs.get('registered_date')
    phone = kwargs.get('phone')
    picture = kwargs.get('picture')

    try:
        session.execute("""
            INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, 
                post_code, email, username, dob, registered_date, phone, picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (user_id, first_name, last_name, gender, address,
              postcode, email, username, dob, registered_date, phone, picture))
        logging.info(f"Data inserted for {first_name} {last_name}")

    except Exception as e:
        logging.error(f'could not insert data due to {e}')


def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
            .config("spark.cassandra.connection.host", "cassandra") \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn

    # s_conn = None
    # try:
    #     import os
    #     os.environ['PYSPARK_SUBMIT_ARGS'] = '--conf spark.driver.extraJavaOptions="-Dio.netty.tryReflectionSetAccessible=true --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED" pyspark-shell'
        
    #     # Create the Spark session with configurations
    #     s_conn = SparkSession.builder \
    #         .appName("SparkDataStreaming") \
    #         .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
    #                                        "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
    #         .config('spark.cassandra.connection.host', 'localhost') \
    #         .config('spark.driver.extraJavaOptions', '-Dio.netty.tryReflectionSetAccessible=true --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED') \
    #         .config('spark.executor.extraJavaOptions', '-Dio.netty.tryReflectionSetAccessible=true --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED') \
    #         .getOrCreate()
         
    #     s_conn.sparkContext.setLogLevel("ERROR")
    #     logging.info("Spark connection created successfully!")
    # except Exception as e:
    #     logging.error(f"Couldn't create the spark session due to exception {e}")
     
    # return s_conn


def connect_to_kafka(spark_conn):
    # .option('kafka.bootstrap.servers', 'localhost:9092')
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'broker:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df


def create_cassandra_connection():
    try:
        # connecting to the cassandra cluster
        # cluster = Cluster(['localhost'])
        auth = PlainTextAuthProvider(username='cassandra', password='cassandra')
        cluster = Cluster(['cassandra'])
        # cluster = Cluster(
        #     contact_points=["cassandra"],  
        #     auth_provider=auth,
        # )
        # cluster = Cluster(contact_points=["cassandra"])
        cas_session = cluster.connect()
        logging.info("✅ Cassandra connection established.")
        print("✅ Cassandra connection established.")
        return cas_session

       
    except Exception as e:
            print(f"❌ Attempt {attempt + 1}/{retries} failed: {e}")
            time.sleep(delay)
    print("❌ All attempts failed.")
    return None

def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)

    return sel


if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        if not spark_df:
            logging.error("Failed to connect to Kafka, spark_df is None")
            exit(1)

        # create selection dataframe from kafka
        logging.info("Creating selection dataframe from Kafka stream...")
        selection_df = create_selection_df_from_kafka(spark_df)
        if selection_df is None:
            logging.error("Failed to create selection DataFrame from Kafka stream")
            exit(1)

        # create cassandra connection
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)

            logging.info("Streaming is being started...")

            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                               .option('checkpointLocation', '/tmp/checkpoint')
                               .option('keyspace', 'spark_streams')
                               .option('table', 'created_users')
                               .start())

            streaming_query.awaitTermination()


    # # First check the Spark and Scala versions
    # check_spark_scala_version()
    
    # # Then proceed with the rest of your code
    # spark_conn = create_spark_connection()
    
    # if spark_conn is not None:
    #     # connect to kafka with spark connection
    #     spark_df = connect_to_kafka(spark_conn)
        
    #     if spark_df is not None:  # Add this check
    #         selection_df = create_selection_df_from_kafka(spark_df)
    #         session = create_cassandra_connection()
            
    #         if session is not None:
    #             create_keyspace(session)
    #             create_table(session)
    #             # ... rest of your code
    #     else:
    #         logging.error("Failed to connect to Kafka, spark_df is None")
