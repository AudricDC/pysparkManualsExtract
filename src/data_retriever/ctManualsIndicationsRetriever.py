from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession


def ctManualsIndicationsRetriever() -> DataFrame:
    """
    See this page : https://stackoverflow.com/questions/36740949/how-to-efficiently-read-data-from-mongodb-and-convert-it-into-sparks-dataframe

    :return: spark dataframe from mongoDB data
    """
    inputUri = "mongodb://user:pwd@localhost:27017/db.collection?authSource=admin"
    jarsPackages = "org.mongodb.spark:mongo-spark-connector_2.11:2.3.2"
    # for query and projection, see : https://stackoverflow.com/questions/62482727/specify-fields-in-pyspark-when-reading-from-mongodb-collection
    pipeline = "{'$match': {}}"
    spark = SparkSession.builder.config("spark.mongodb.input.uri", inputUri).config("spark.jars.packages",
                                                                                    jarsPackages).getOrCreate()
    data = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("pipeline", pipeline).load().select("sn",
                                                                                                               "proximities",
                                                                                                               "verticalAlignments",
                                                                                                               "horizontalAlignments")
    # data.select("annotations.type", "annotations.location.mmBladeBaryCenterX").show(truncate=False)
    return data


if __name__ == "__main__":
    ctManualsIndicationsDf = ctManualsIndicationsRetriever()
    ctManualsIndicationsDf.show()
