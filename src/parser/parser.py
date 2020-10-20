import datetime as dt

from pyspark.sql.dataframe import DataFrame
from src.data_retriever.dataRetriever import dataRetriever

from pyspark.sql import Row


def _parseRow(row):
    # get sn
    sn = row.sn
    # parse alleviated column
    isAlleviated = False
    if row.isAlleviated == "YES":
        isAlleviated = True
    # get annotations count
    annotationsCount = int(len(row.annotations))
    # get asc plant
    ascPlant = row.ascPlant
    # parse loomLJ and dateLJ
    try:
        operation = list(filter(lambda x: not x.workCenter.find("LJ"), row.operations))[0]
        loomLJ = operation.workCenter
        dateLJ = operation.date
        # print(dateLJ)
    except IndexError:
        loomLJ = "Unknown"
        dateLJ = dt.datetime(1999, 1, 1)
    # proximities, alignments set to 0 because of no data
    return Row(qty=1, sn=sn, isAlleviated=isAlleviated, tomoPlant=row.plant, annotationsCount=annotationsCount,
               ascPlant=ascPlant, loomLJ=loomLJ, dateLJ=dateLJ, nbOfProximities=0, nbOfVAlignments=0, nbOfHAlignments=0)


def dataParser(data: DataFrame) -> DataFrame:
    csvData = data.rdd.map(_parseRow).toDF()
    return csvData


if __name__ == "__main__":
    data = dataRetriever()
    csvData = dataParser(data)
    csvData.show()
