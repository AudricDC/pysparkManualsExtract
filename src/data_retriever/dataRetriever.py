from pyspark.sql.dataframe import DataFrame
from src.data_retriever.ctManualsRetriever import ctManualsRetriever
from src.data_retriever.ascRetriever import ascRetriever


def dataRetriever() -> DataFrame:
    ctManualsDf = ctManualsRetriever()
    ascDf = ascRetriever()
    mergedDf = ctManualsDf.join(ascDf, on="sn", how="inner")
    return mergedDf


if __name__ == "__main__":
    df = dataRetriever()
    df.show()
