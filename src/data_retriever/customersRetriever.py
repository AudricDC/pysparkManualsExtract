import os
from configparser import ConfigParser

from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode


def customersRetriever() -> DataFrame:
    """
    See this page : https://stackoverflow.com/questions/36740949/how-to-efficiently-read-data-from-mongodb-and-convert-it-into-sparks-dataframe

    :return: spark dataframe from mongoDB data
    """
    try:
        config = ConfigParser()
        file_path = os.path.realpath(__file__)
        conf_path = os.path.join(os.path.dirname(file_path), r"mongo.ini")
        config.read(conf_path)
    except:
        config = ConfigParser()
        project_path = os.getcwd()
        module_path = r"mongo.ini"
        config.read(os.path.join(project_path, module_path))
    username = config['MONGO_AUTHENTICATION']['USER']
    pwd = config['MONGO_AUTHENTICATION']['PWD']

    atlasUri = "mongodb+srv://{username}:{pwd}@cluster0.qlwam.mongodb.net/sample_analytics.customers?retryWrites=true".format(
        username=username, pwd=pwd)
    jarsPackages = "org.mongodb.spark:mongo-spark-connector_2.11:2.3.2"
    # for query and projection, see : https://stackoverflow.com/questions/62482727/specify-fields-in-pyspark-when-reading-from-mongodb-collection
    pipeline = "{'$match': {}}"
    spark = SparkSession.builder.config("spark.mongodb.input.uri", atlasUri).config("spark.jars.packages",
                                                                                    jarsPackages).getOrCreate()
    data = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("pipeline", pipeline).load(). \
        select("name", "address", explode("accounts").alias("account_id"))
    # data.select("annotations.type").show(truncate=False)
    return data


if __name__ == "__main__":
    customersDf = customersRetriever()
    customersDf.show()
