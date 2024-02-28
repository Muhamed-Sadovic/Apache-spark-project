from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd


spark = SparkSession.builder.appName("Projekat").getOrCreate()

data = spark.read.csv("kupovina.csv", header=False, inferSchema=True)

data = data.toDF("id_potrosaca", "oznaka_proizvoda", "cena")

ukupna_potrosnja = data.groupBy("id_potrosaca").agg(F.sum("cena").alias("ukupna_potrosnja"))

result = pd.DataFrame(ukupna_potrosnja.select("id_potrosaca", "ukupna_potrosnja").orderBy("id_potrosaca").collect(), columns=["id_potrosaca", "ukupna_potrosnja"])

print(result.to_string(index=False, header=True))

spark.stop()