import glob
import json
import logging
import re
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

logger = logging.getLogger(__name__)


class EtlMethods:
    """
    Здесь собраны основные методы для выполнения основных ETL функций
    """

    @staticmethod
    def get_hdfs_location(spark: SparkSession, table_name: str) -> str:
        """
        Возвращает путь по которому таблица физически лежит в hdfs
        Args:
            spark - Спарк сессия
            table_name - Полное название таблицы (схема + таблица)

        Returns:
            Путь, по которому таблица физически лежит в hdfs
        Пример:
            'hdfs://sna-dev/data/custom/salesntwrk/models_dm_sverka/pa/txn_fraud'
        """
        query = f"DESC FORMATTED {table_name}"

        df = spark.sql(query)
        df = df.filter("col_name=='Location'")
        hdfs_location = df.collect()[0].data_type

        return hdfs_location

    @staticmethod
    def cnt_rows(spark: SparkSession, schema: str, table: str, condition: str = "1=1") -> int:
        """
        Находит число строк в указаной таблице

        Args:
            spark: спарк сессия
            table:  название таблицы
            schema:  название азы данных
            condition: фильтр датфрейма по условию (опцонально)
        Returns:
            Число строк указанного датаферйма
        """
        logger.debug("Read hive table")
        df = spark.table(f"{schema}.{table}")
        num_rows = df.filter(condition).count()
        logger.debug("Result of query: " + str(num_rows))
        return num_rows

    @staticmethod
    def get_max_value(spark: SparkSession, schema: str, table_name: str, col_name: str, condition: str = "1=1"):
        """
        Находит максимальное значение указанного элемента с применением фильтрации (если она есть)

        Args:
            spark: спарк сессия
            schema: название базы данных/схемы
            table_name: название таблицы
            col_name: название столбца, по которому производится вычисление
            condition: условие для фильтрации

        Returns:
            Максимальное значение элемента
        """
        logger.debug("Start to compute max value")
        df = spark.table(f"{schema}.{table_name}")
        try:
            res = df.filter(condition) \
                .agg({col_name: "max"}) \
                .first()[0]
        except TypeError:
            logging.warning(f"There are NO data in table = {table_name} with condition {condition}")
            raise

        return res

    @staticmethod
    def insert_single_row_data(spark: SparkSession, db: str, table: str, **kwargs):
        """
        Добавляет переданные данные в таблицу.
        Берёт схему из указанной таблицы и на основании данной схемы формирует собственный датаферйм и загружет его
        Args:
            spark: SparkSession спарк сессия
            db: Название схемы
            table: Название таблицы
            **kwargs: данные, которые нужно загрузить
        Requre:
            Название переменных должно полностью совпадать с названием столбцов!
        """
        target_name = f"{db}.{table}"

        data = []
        value = {}

        logger.debug("read schema")
        df = spark.table(target_name)
        schema = df.schema

        for row in schema:
            value[row.name] = kwargs.get(row.name)

        data.append(value)
        logger.debug(f"create df with value = {value}")
        insert_df = spark.createDataFrame(data, schema)

        logger.debug("start to append data in table")
        insert_df.write.insertInto(target_name, overwrite=False)
        logger.info(f"add row {str(value)} in Kpd Report")
        return None

    @staticmethod
    def get_partitions_df(spark: SparkSession, table_db: str, table_name: str):
        """
        Возвращает отсортированный по убыванию датафрейм со всеми значениями партиций:
        Пример:
        +------------------------------------------------+
        |partition                                       |
        +------------------------------------------------+
        |platformcolumn=MOBILE/timestampcolumn=2022-10-05|
        |platformcolumn=MOBILE/timestampcolumn=2022-10-04|
        +------------------------------------------------+
        Args:
            spark: Спарк сессия
            table_db: название схемы
            table_name: название таблицы
        Returns:
            Датафрейм со значениями партиций
        """
        query = f"show partitions {table_db}.{table_name}"
        df = spark.sql(query)
        return df

    @staticmethod
    def get_last_partition(spark: SparkSession, table_db: str, table_name: str):
        """
        Возвращает последнюю актуальную партицию
        Args:
            spark: Спарк сессия
            table_db: название схемы
            table_name: название таблицы
        Returns:
            Датафрейм со значениями партиций
        """
        df = EtlMethods.get_partitions_df(spark, table_db, table_name)
        df = df.orderBy(f.col("partition").desc())
        last_partition = df.first().partition
        return last_partition


class CTLStatistics:
    def __init__(self, ctl_url: str, loading_id: int, ctl_entity_id: int):
        """
        Args:
            ctl_url: url для обращений к CTL по rest
            loading_id: id конкретного запуска потока
            ctl_entity_id: id cущности куда статистики публиковать
        """
        self._ctl_url = ctl_url
        self._loading_id = loading_id
        self._ctl_entity_id = ctl_entity_id

    def upload_statistic(self, stat_id: int, stat_value: str) -> None:
        """
        Публикация статистики в CTL

        Args:
            stat_id: номер публикуемой статистики (см в CTL)
            stat_value: значение публикуемой статистики

        Returns: None
        """
        logger.debug("upload stat with stat_id: {}".format(stat_id))

        data = {"loading_id": int(self._loading_id),
                "entity_id": int(self._ctl_entity_id),
                "stat_id": int(stat_id),
                "avalue": [str(stat_value)]}

        header = {"Content-Type": "application/json"}

        response = requests.post(url=self._ctl_url,
                                 data=json.dumps(data),
                                 headers=header)

        logger.debug("response status code: {}".format(response.status_code))
        if not response.ok:
            logger.error("ctl statistic with id: {} not uploading".format(stat_id))

        return None


class FileWithStep:
    """
    Класс в котором хранятся sql-ные файлы

    Которые должны запускаться в определенном порядке
    Файл с наименьшим step должен запускаться первым

    Например
    1) step = 1
    2) step = 2
    3) step = 13

    Значение step должно принадлежать [0; +infinity)
    """

    def __init__(self, path_to_file: str, step: int):

        assert isinstance(step, int)

        if step < 0:
            raise ValueError("step should be [0; +infinity)")

        self.path_to_file = path_to_file
        self.step = step

    def __lt__(self, other):
        if self.step < other.step:
            return True
        else:
            return False


def get_spark(app_name: str) -> SparkSession:
    """
    Получение спарк сессии

    Args:
        app_name:
    Returns: pyspark.sql.SparkSession
    """

    spark = (SparkSession.builder
             .appName(app_name)
             .enableHiveSupport()
             .getOrCreate())

    spark.sparkContext.setLogLevel('ERROR')

    blockSize = 1024 * 1024 * 128
    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().setInt("df.blocksize", blockSize)
    sc._jsc.hadoopConfiguration().setInt("parquet.blocksize", blockSize)
    sc._jsc.hadoopConfiguration().setInt("dfs.blocksize", blockSize)
    sc._jsc.hadoopConfiguration().setInt("dfs.block.size", blockSize)

    return spark


def get_sql_query(fname: str) -> str:
    """
    Читает sql-ный запрос из файла

    Args:
        fname: путь до файла с sql Запросом

    Returns: sql-ный запрос

    """
    with open(fname) as f:
        query = f.read()
    return query


# TODO удалил методы связанные с логированием, убедиться, что ничего не сломалось , если что - вернуть их
def extract_all_files_by_pattern_with_step(path_to_scanning: str, pattern: str) -> list:
    """
    Па узазанному пути извлекаются все файы удовлетовряющие шаблону

    Важно, чтобы в назнвании самого файла было именно одно число - шаг

    Например:
        корректные названия: stat1.sql, 1stat.sql, stat12.sql
        не корректные названия: stat1_1.sql, 12stat1.sql

    Args:
        path_to_scanning: директория в которой нужно файлы искать (рекурсивно)
        pattern: шаблон интересующих файлов

    Returns: список файлов (обьекты типа SQLFileWithStep)

    Пример
        path_to_scanning:
            * "."
            * ".."
            * "/"
            * "/scripts"
            * "/user/home/scripts"

        pattern:
            * "step*.sql"
            * "*.sql"
    """
    all_files = []

    for file in Path(path_to_scanning).rglob(pattern):
        absolute_file_path = str(file.absolute())
        file_name = file.name

        step = re.findall(r"\d+", file_name)

        if len(step) != 1:
            raise ValueError("Expect file with one digit(step), but get {}".format(step))

        file_with_step = FileWithStep(absolute_file_path, int(step[0]))
        all_files.append(file_with_step)

    return all_files


def get_grouped_sql_files_by_pattern_with_step(path_to_scanning: str, pattern: str) -> list:
    """
    Данный метод группирует список файлов (обьекты типа SQLFileWithStep) по одинаковому шагу file.step

    Args:
        path_to_scanning: директория в которой нужно файлы искать (рекурсивно)
        pattern: шаблон интересующих файлов

    Returns: Отсортированный и сгруппированный по шагам список кортежей формата (шаг, список sql файлов этого шага) .
    Пример: [(1, [/sql/step_1_insert_stg_features.sql, script/step_1_insert_stg_features.sql]),
             (2, [/sql/step_2_insert_stg.sql, /sql/step_2.sql]),
             (3, [sql/step_3.sql])
            ]

    Пример:
        path_to_scanning:
            * "."
            * ".."
            * "/"
            * "/scripts"
            * "/user/home/scripts"

        pattern:
            * "step*.sql"
            * "*.sql"
    """
    files = extract_all_files_by_pattern_with_step(path_to_scanning, pattern)
    steps = set(map(lambda x: x.step, files))
    grouped_sql = []
    for step in steps:
        step_list = [y.path_to_file for y in files if y.step == step]
        grouped_sql.append((step, step_list))

    return grouped_sql


def recreate_table(spark: SparkSession, path_to_query: str, **kwargs) -> None:
    logger.info("Recreate tables")
    query_files = glob.glob(path_to_query)
    if len(query_files) == 0:
        logger.error("There are not files on path: " + path_to_query)

    for file_name in query_files:
        queries = get_sql_query(file_name)
        queries = queries.split(';')
        for query in queries:
            spark.sql(query.format(**kwargs))


def execute_sql_query_from(spark: SparkSession, path_to_query: str, **kwargs):
    """
    Исполняет запрос из указанного файла

    Благодоря тому, что исполняет переданный запрос
    Args:
        spark: спарк сессия
        path_to_query: адрес файла с sql-ным запросом
        argv: параметры для форматирования запроса

    """
    queries = get_sql_query(path_to_query).format(**kwargs)
    queries = queries.split(';')
    for query in queries:
        logger.debug("Executing query:\n" + query)
        try:
            spark.sql(query)
        except Exception as e:
            logger.error(f"file {path_to_query} has not been executed!")
            raise
    return None


def execute(spark: SparkSession, path_to_file: str, **kwargs):
    """
    Метод выполняет любой ETL файл.
    Args:
        spark: спарк сессия
        path_to_file: путь до ETL файла
        **kwargs: параметры для форматирования запроса
    """
    # TODO Прокинуть логи!
    if Path(path_to_file).suffix == ".sql":
        execute_sql_query_from(spark=spark, path_to_query=path_to_file, **kwargs)
    elif Path(path_to_file).suffix == ".py":

        exec(Path(path_to_file).read_text() % kwargs)
    else:
        raise ValueError("Extension not in (.sql , .py)")
    return None


def parallel_execution_queries_from_list(spark: SparkSession, queries: list, **kwargs):
    """
    Выполняет параллельный рассчёт sql файлов, которые передаются на вход функции
    Args:
        spark: спарк сессия
        queries: список сгриппированных по шагу sql файлов. См пример в get_queue_grouped_sql_files_by_pattern_with_step
        **kwargs: параметры для форматирования запроса
    Requires:
        Формат списка sql файлов должен быть в формате list[  list[ [path/query1, path/query2], [..] ]  ]
    """
    logger.debug("start to execute all queries")
    logger.info(queries)
    num_jobs = len(queries)
    with ThreadPoolExecutor(num_jobs) as executor:
        jobs_list = []

        for path_to_sql in queries:
            job = executor.submit(execute, spark, path_to_sql, **kwargs)
            jobs_list.append(job)
        logger.info("check job list")
        for job in as_completed(jobs_list):
            result = job.result()
            logger.info("Result for some job:\t" + str(result))
    return None


class KpdReport(EtlMethods):
    """
    Класс kpd report. Выполняет все операции, связанные с таблицей kpd_report
    """

    def __init__(self, spark: SparkSession, **kwargs):
        """
        Args:
            spark: Спарк сессия. Необходима для операций с таргет таблицей и с таблицей kpd_report
        """
        self.spark = spark
        self.target_db = kwargs["target_db"]
        self.target_table = kwargs["target_table"]
        self.kpd_table_name = kwargs["kpd_table_name"]

    # TODO Написать проверки на максимальную партицию
    def get_last_computed_step(self, is_recovery_mode):
        """
        Находит последний выполненный шаг в соответствии с выбранным режимом
        Args:
            is_recovery_mode: Параметр для выполнения восстановительного режима
        Returns:
            Номер последнего выполненного шага
        """
        last_computed_step = 0
        if is_recovery_mode:
            last_ctl_validrom = self.get_max_value(self.spark, self.target_db, self.kpd_table_name, "ctl_validfrom")
            last_computed_step = self.get_max_value(spark=self.spark,
                                                    schema=self.target_db,
                                                    table_name=self.kpd_table_name,
                                                    col_name="last_success_step",
                                                    condition=f"ctl_validfrom = '{last_ctl_validrom}' ")
        return last_computed_step

    def get_previous_count_of_records(self):
        """
        Находит число строк в самой последней таблице.
        Сортирует строки по последней загрузке ctl_validfrom = max(ctl_validfrom)
        Returns:
            Число строк
        Requires:
            Таблица kpd_report подрузамеавет, что последняя дата загрузки загружает именно последнюю партицию. В противном случае kpd_report не будет работать!
            Метод предполагает, что у нас есть только одна таргет таблица

        """
        last_ctl_validrom = self.get_max_value(self.spark, self.target_db, self.kpd_table_name, "ctl_validfrom")
        full_kpd_table_name = f"{self.target_db}.{self.kpd_table_name}"
        previous_count_of_records = self.cnt_rows(self.spark, self.target_db, self.kpd_table_name,
                                                  condition=f"ctl_validfrom = '{last_ctl_validrom}' ")
        # TODO тут написать методы проверки на max partition == partition where ctl_validfrom = '{last_ctl_validrom}'
        # TODO прописать логи
        return int(previous_count_of_records)

# TODO переписать в main.py функции, на вызов из класса, дописать kpd_report
