# MongoDB extract to CSV using pyspark

A simple extract of MongoDB data to load it into a CSV file, using pyspark.


## Requirements

See requirements.txt

## Data set 

Data set is from MongoDB Atlas.
Atlas provides sample data you can load into an Atlas cluster.
To utilize the sample data provided by Atlas, you must create an Atlas cluster to load data into.
To get started with Atlas : https://docs.atlas.mongodb.com/getting-started/
Then load sample data : https://docs.atlas.mongodb.com/sample-data/

I created a cluster named "cluster0" (default name when following the step while getting started with Atlas) 
and loaded all the sample datasets. 
I now have several databases including : sample_analytics, containing three collections : 
accounts, customers and transactions.
Here, one will focus on customers and transactions collections.

Customers collection contains information on a list of customers, like : name, username, address, birthdate, etc ...
And also the list of accounts owned by the customer, which is a list of account id.
Transactions collection contains the list of transactions and its details for each account id.

With these two informations gathered, the objectif is to determine the balance of the transactions
for every customer.

## First install requirements.txt

Run in command line :

```
pip install requirements.txt
```

## Atlas mongo manager

I create a mongo.ini which contains my authentication information (username and pwd) to connect to my cluster.
Then I create an instance of MongoDB with python which connect to Atlas, using a string URI :

```python
atlasUri = "mongodb+srv://{username}:{pwd}@cluster0.qlwam.mongodb.net/sample_analytics.collection?retryWrites=true". \
        format(username=username, pwd=pwd)
```

Note : I do it twice, replacing collection by customers and transactions.

Then, I initialize a SparkSession to connect to Atlas, and load information.

```python
# customers retriever
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
jarsPackages = "org.mongodb.spark:mongo-spark-connector_2.11:2.3.2"
pipeline = "{'$match': {}}"
spark = SparkSession.builder.config("spark.mongodb.input.uri", atlasUri).config("spark.jars.packages", jarsPackages).getOrCreate()
data = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("pipeline", pipeline).load().select("name", "address", explode("accounts").alias("account_id"))
```

```python
# transactions retriever
from pyspark.sql import SparkSession
jarsPackages = "org.mongodb.spark:mongo-spark-connector_2.11:2.3.2"
pipeline = "{'$match': {}}"
spark = SparkSession.builder.config("spark.mongodb.input.uri", atlasUri).config("spark.jars.packages", jarsPackages).getOrCreate()
data = spark.read.format("com.mongodb.spark.sql.DefaultSource").load().select("account_id", "transaction_count", "transactions.amount", "transactions.transaction_code")
```

## Data retriever

The two parts above are contained in two several functions called customersRetiever and transactionsRetriever.

I created a function called dataRetriever in src/data_retriever/dataRetriever.py which join the two data sets.

```python
from pyspark.sql.dataframe import DataFrame
from src.data_retriever.customersRetriever import customersRetriever
from src.data_retriever.transactionsRetriever import transactionsRetriever
def dataRetriever() -> DataFrame:
    customersDf = customersRetriever()
    transactionsDf = transactionsRetriever()
    mergedDf = customersDf.join(transactionsDf, on="account_id", how="inner")
    return mergedDf
```

## Data parser

I have the following src/data_parser/parser.py script, which parses each row of the data to return a new DataFrame:
 
```python
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

```
 
## main.py
 
Finally, my main.py calls dataRetriever, dataParser and save the DataFrame into a csv file:


  ```python
from src.data_retriever.dataRetriever import dataRetriever
from src.parser.parser import dataParser
  
if __name__ == "__main__":
    data = dataRetriever()
    csvData = dataParser(data)
    csvData.repartition(1).write.format('com.databricks.spark.csv').save("myfile.csv", header='true')  # faster
    input("click")  # too see time on spark UI
```

## Output

Finally, in a "myfile.csv" folder, I can find a csv file which looks like :

![Alt text](output_file.PNG?raw=true "Title")

