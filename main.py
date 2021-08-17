from pyspark.sql import SparkSession

from DataManager import DataManager
from Solver import Solver

spark = SparkSession.builder.appName('CaseOne').getOrCreate()

dataManager = DataManager(spark)

fecha = dataManager.read_parquet_files("fecha")
retrasos = dataManager.read_parquet_files("retrasos")
vuelos = dataManager.read_parquet_files("vuelos")

solver = Solver(spark, vuelos, fecha, retrasos)

""" 1. ¿De qué país salieron más aviones? """
solver.get_max_countries_by_origin()

""" 2. ¿A qué país llegaron más aviones? """
solver.get_max_countries_by_destiny()

""" 3. ¿Qué día hubo más y menos vuelos? """
solver.get_count_flights_by_day()

""" 4. ¿Qué día hubo más y menos retrasos? """
solver.get_count_delays_by_day()

""" 6. """
solver.show_vip_countries()

""" 7.
solver.save_vip_to_one_file()
solver.save_vip_to_ten_files()
"""