from pyspark.sql.functions import col, asc, desc, udf, array
from pyspark.sql.types import StringType

COUNTRY_IS_VIP = 'YES'
COUNTRY_IS_NOT_VIP = 'NO'


def get_max_min_count(data_frame, operation='max'):
    return data_frame.agg({'count': operation}).first()[0]


def print_header(question_number):
    print("QUESTION " + question_number)


class Solver:
    FILES_PATH = "C:/BigDataLocalSetup/data/savedRecords/"

    def __init__(self, spark_session, vuelos, fecha, retraso):
        self.spark = spark_session
        self.vuelos = vuelos
        self.fecha = fecha
        self.retraso = retraso

    def get_flights_by_country(self, group_column, order_by='max'):
        flights_grouped_df = self.vuelos.groupBy(group_column).count().orderBy(col('count').desc())
        max_day = get_max_min_count(flights_grouped_df, order_by)

        return flights_grouped_df.filter(col('count') == max_day)

    def get_max_countries_by_origin(self):
        """
        Método que obtiene de qué país salieron más aviones
        """
        flights_by_country_and_origin = self.get_flights_by_country('origen')

        print_header("1")
        flights_by_country_and_origin.show()

    def get_max_countries_by_destiny(self):
        flights_by_country_and_destiny = self.get_flights_by_country('destino')

        print_header("2")
        flights_by_country_and_destiny.show()

    def get_count_flights_by_day(self):
        flights_by_day = self.fecha.groupBy('dia').count()
        day_most_flights = flights_by_day.filter(col('count') == get_max_min_count(flights_by_day, 'max'))
        day_less_flights = flights_by_day.filter(col('count') == get_max_min_count(flights_by_day, 'min'))

        print_header("3")
        day_most_flights.show()
        day_less_flights.show()

    def get_count_delays_by_day(self):
        delays_by_day = self.retraso.join(self.fecha, self.retraso.vuelo == self.fecha.num_vuelo)
        delays_by_day = delays_by_day.groupBy('dia').count()
        day_most_delays = delays_by_day.filter(col('count') == get_max_min_count(delays_by_day, 'max'))
        day_less_delays = delays_by_day.filter(col('count') == get_max_min_count(delays_by_day, 'min'))

        print_header("4")
        day_most_delays.show()
        day_less_delays.show()

    @staticmethod
    @udf(returnType=StringType())
    def is_vip(list_search, field_search='code'):
        vip_countries = {'ESP': 'España', 'PER': 'Perú', 'MEX': 'México'}
        if field_search == 'name':
            vip_countries = vip_countries.values()

        for item in list_search:
            if item in vip_countries:
                return COUNTRY_IS_VIP

        return COUNTRY_IS_NOT_VIP

    def get_vip_countries(self):
        return self.vuelos.withColumn("pais_vip", self.is_vip(array(col('origen'), col('destino'))))

    def show_vip_countries(self):
        paises_vip = self.get_vip_countries()

        print_header("6")
        paises_vip.show()

    def save_vip_to_parquet(self, num_files):
        folder_name = 'repartition_' + str(num_files)
        paises_vip = self.get_vip_countries()
        paises_vip.repartition(num_files).write.parquet(self.FILES_PATH + folder_name)

    def save_vip_to_one_file(self):
        self.save_vip_to_parquet(1)

    def save_vip_to_ten_files(self):
        self.save_vip_to_parquet(10)
