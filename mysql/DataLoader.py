import configparser

from pyspark.sql import SparkSession


def checkTableDataCount(spark, MYSQL_DRIVER, DB_URL, DB_USER, tableName):
    print(tableName + " Table Data Count ")
    tables = spark.read \
        .format("jdbc") \
        .option("driver", MYSQL_DRIVER) \
        .option("url", DB_URL) \
        .option("query", "select count(1) from " + tableName) \
        .option("user", DB_USER) \
        .load()
    tables.show()


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read("../conf/db.properties")

    MYSQL_DRIVER = config.get("DB", "MYSQL_DRIVER")
    DB_URL = config.get("DB", "DB_URL")
    DB_USER = config.get("DB", "DB_USER")

    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("SparkByExamples.com") \
        .config("spark.jars", "mysql-connector-j-8.0.33.jar") \
        .getOrCreate()

    # checkTables(spark, MYSQL_DRIVER, DB_URL, DB_USER)

    leaseDetail = spark.read.csv("../inputFiles/LeaseDetails/*.csv", header=True)
    leaseSales = spark.read.csv("../inputFiles/LeaseSales/*.csv", header=True)
    leaseTrans = spark.read.csv("../inputFiles/LeaseTrans/*.csv", header=True)

    print("Inserting data into leaseDetails Count : " + str(leaseDetail.count()))
    print("Inserting data into leaseSales Count : " + str(leaseSales.count()))
    print("Inserting data into leaseTrans Count : " + str(leaseTrans.count()))

    leaseDetail.write \
        .mode("APPEND") \
        .format("jdbc") \
        .option("driver", MYSQL_DRIVER) \
        .option("url", DB_URL) \
        .option("dbtable", "leaseDetails") \
        .option("user", DB_USER) \
        .save()

    leaseSales.write \
        .mode("APPEND") \
        .format("jdbc") \
        .option("driver", MYSQL_DRIVER) \
        .option("url", DB_URL) \
        .option("dbtable", "leaseSales") \
        .option("user", DB_USER) \
        .save()

    leaseTrans.write \
        .mode("APPEND") \
        .format("jdbc") \
        .option("driver", MYSQL_DRIVER) \
        .option("url", DB_URL) \
        .option("dbtable", "leaseTrans") \
        .option("user", DB_USER) \
        .save()

    checkTableDataCount(spark, MYSQL_DRIVER, DB_URL, DB_USER, "leaseDetails")
    checkTableDataCount(spark, MYSQL_DRIVER, DB_URL, DB_USER, "leaseSales")
    checkTableDataCount(spark, MYSQL_DRIVER, DB_URL, DB_USER, "leaseTrans")
