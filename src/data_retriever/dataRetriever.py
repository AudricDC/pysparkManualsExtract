from pyspark.sql.dataframe import DataFrame
from src.data_retriever.customersRetriever import customersRetriever
from src.data_retriever.transactionsRetriever import transactionsRetriever


def dataRetriever() -> DataFrame:
    customersDf = customersRetriever()
    transactionsDf = transactionsRetriever()
    mergedDf = customersDf.join(transactionsDf, on="account_id", how="inner")
    return mergedDf


if __name__ == "__main__":
    df = dataRetriever()
    df.show()
