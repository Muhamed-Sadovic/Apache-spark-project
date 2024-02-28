from pyspark import SparkConf, SparkContext
import pandas as pd

conf = SparkConf().setAppName("Projekat").setMaster("local")
sc = SparkContext(conf=conf)

PATH = r"C:\Users\MUKI\Desktop\projekatDI\kupovina.csv"
lines = sc.textFile(PATH) 


data_rdd = lines.map(lambda line: line.split(",")).map(lambda x: (int(x[0]), float(x[2])))

ukupna_potrosnja_rdd = data_rdd.reduceByKey(lambda x, y: x + y)

ukupna_potrosnja_rdd = ukupna_potrosnja_rdd.sortByKey()

result_df = pd.DataFrame(ukupna_potrosnja_rdd.collect(), columns=["oznaka_potrosaca", "ukupna_potrosnja"])

print(result_df.to_string(index=False, header=True))

sc.stop()