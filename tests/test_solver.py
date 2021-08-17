from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from Solver import Solver, get_max_min_count

spark = SparkSession.builder.appName('CaseOne_test').getOrCreate()


def test_instance_Solver():
    spark_session = ""
    vuelos = []
    fecha = []
    retraso = []

    s = Solver(spark_session, vuelos, fecha, retraso)


def create_vuelos_dummy():
    dummy_flights = [(1000, 'ESP', 'ARG'), (2000, 'PER', 'CHI'), (3000, 'MEX', 'USA')]
    dummy_schema = StructType(
        [StructField("vuelo", IntegerType()), StructField("origen", StringType()),
         StructField("destino", StringType())])
    return spark.createDataFrame(data=dummy_flights, schema=dummy_schema)


def create_fecha_dummy():
    dummy_dates = [(1000, 1), (2000, 2), (3000, 3)]
    dummy_schema = StructType([StructField("vuelo", IntegerType()), StructField("dia", IntegerType())])
    return spark.createDataFrame(data=dummy_dates, schema=dummy_schema)


def create_retraso_dummy():
    dummy_delays = [(1000, 5), (2000, 10), (3000, 15)]
    dummy_schema = StructType([StructField("vuelo", IntegerType()), StructField("retraso", IntegerType())])
    return spark.createDataFrame(data=dummy_delays, schema=dummy_schema)


def test_get_max_count():
    dummy_dates = [(1, 10), (2, 5), (3, 1)]
    dummy_schema = StructType([StructField("day", IntegerType()), StructField("count", IntegerType())])
    dummy_df = spark.createDataFrame(data=dummy_dates, schema=dummy_schema)
    max_count = get_max_min_count(dummy_df, "max")

    assert dummy_df.count() != 0
    assert max_count == 10


def test_max_flights_by_origin():
    s = Solver(spark, create_vuelos_dummy(), create_fecha_dummy(), create_retraso_dummy())
    assert s.get_flights_by_country('origen', 'max').select('count').first()[0] == 1


def test_max_flights_by_destiny():
    s = Solver(spark, create_vuelos_dummy(), create_fecha_dummy(), create_retraso_dummy())
    assert s.get_flights_by_country('destino', 'max').select('count').first()[0] == 1


def test_is_vip():
    s = Solver(spark, create_vuelos_dummy(), create_fecha_dummy(), create_retraso_dummy())
    vip_countries = s.get_vip_countries()

    assert vip_countries.count() != 0
    assert vip_countries.select('pais_vip').first()[0] == 'YES'
