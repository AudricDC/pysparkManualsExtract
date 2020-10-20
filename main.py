from src.data_retriever.dataRetriever import dataRetriever
from src.parser.parser import dataParser


def _toCSVLine(data):
    return ','.join(str(d) for d in data)


def mainUsingRDD():
    # csvData.rdd.map(lambda x: ",".join(map(str, x))).coalesce(1).saveAsTextFile("file.csv")
    # or
    # lines = csvData.rdd.map(_toCSVLine)
    # lines.saveAsTextFile('mycsv.csv')
    return


if __name__ == "__main__":
    data = dataRetriever()
    csvData = dataParser(data)
    csvData.repartition(1).write.format('com.databricks.spark.csv').save("myfile.csv", header='true')  # faster
    input("click")  # too see time on spark UI
