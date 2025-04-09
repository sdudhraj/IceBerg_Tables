# s3_to_iceberg.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp


def main():
    ## Configurations for Spark Session

    ## Start Spark Session
    spark = SparkSession.builder.appName("Iceberg Example").getOrCreate()
    print("Spark Running")

    ## SKIPING READING FROM MINIO
    ## CREATE DATEFRAME
    data = [(7, "Mark"), (8, "Jerry"), (9, "Lila")]
    columns = ["id", "name"]
    df = spark.createDataFrame(data, columns).withColumn("ts", current_timestamp())
    # DATAFRAME ENDS

    # Step 3: Create the "db" namespace in Nessie
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.db")
    print("Namespace 'db' created in Nessie")


    # Step 4: Create an empty Iceberg table using SQL
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.db.sampletable (
            id INT,
            name STRING,
            ts TIMESTAMP
        )
        USING iceberg
    """)
    print("Iceberg table created with no records")

    # Step 5: Insert the data from JSON DataFrame into the Iceberg table
    df.writeTo("nessie.db.sampletable").append()
    print("Data inserted into Iceberg table from CSV file")

    #query table
    spark.sql("SELECT * FROM nessie.db.sampletable;").show()

    # Spark stop
    spark.stop()

if __name__ == "__main__":
    main()