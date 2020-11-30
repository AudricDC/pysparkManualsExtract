from src.data_retriever.dataRetriever import dataRetriever
from src.parser.parser import dataParser

if __name__ == "__main__":
    data = dataRetriever()
    csvData = dataParser(data)
    csvData.repartition(1).write.format('com.databricks.spark.csv').save("myfile.csv", header='true')  # faster
    input("click")  # too see time on spark UI
