from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession


def ascRetriever() -> DataFrame:
    """
    See this page : https://stackoverflow.com/questions/36740949/how-to-efficiently-read-data-from-mongodb-and-convert-it-into-sparks-dataframe

    :return: spark dataframe from mongoDB data
    """
    inputUri = "mongodb://audric:ixep@localhost:27017/asMoldedEngineeringData.ascData?authSource=admin"
    jarsPackages = "org.mongodb.spark:mongo-spark-connector_2.11:2.3.2"
    spark = SparkSession.builder.config("spark.mongodb.input.uri", inputUri).config("spark.jars.packages",
                                                                                    jarsPackages).getOrCreate()
    data = spark.read.format("com.mongodb.spark.sql.DefaultSource").load().select("sn", "plant", "operations").withColumnRenamed("plant", "ascPlant")
    # data.select("annotations.type", "annotations.location.mmBladeBaryCenterX").show(truncate=False)
    return data


if __name__ == "__main__":
    ascDf = ascRetriever()
    ascDf.show()