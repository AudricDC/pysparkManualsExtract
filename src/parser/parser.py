from pyspark.sql.dataframe import DataFrame
from src.data_retriever.dataRetriever import dataRetriever

from pyspark.sql import Row


def __getAmount(amount, transaction_code):
    if transaction_code == "sell":
        return amount
    return -amount


def _parseRow(row):
    name = row.name
    address = row.address
    account_id = row.account_id
    transaction_count = row.transaction_count
    transaction_balance = sum(
        map(lambda x, y: __getAmount(amount=x, transaction_code=y), row.amount, row.transaction_code))
    return Row(name=name, address=address, account_id=account_id, transaction_count=transaction_count,
               transaction_balance=transaction_balance)


def dataParser(data: DataFrame) -> DataFrame:
    csvData = data.rdd.map(_parseRow).toDF()
    csvData = csvData.groupBy("name").agg(
        {"transaction_balance": "sum", "transaction_count": "sum", "account_id": "count"})
    return csvData


if __name__ == "__main__":
    data = dataRetriever()
    csvData = dataParser(data)
    csvData.show()
