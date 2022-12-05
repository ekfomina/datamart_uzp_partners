"""
В данный модуль вынесен функционал, связанный управлением партициями
"""
import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import operator
from dateutil import relativedelta

logger = logging.getLogger(__name__)


class PartitionController:
    def __init__(self, spark, start_wf_time):
        fs = spark.sparkContext._jvm.org.apache.hadoop.fs
        hadoopConfiguration = spark.sparkContext._jsc.hadoopConfiguration()

        self._spark = spark
        self._start_wf_time = start_wf_time
        self._Path = fs.Path
        # self._parquet_filter = fs.GlobFilter("*.parquet")
        self._file_system = fs.FileSystem.get(hadoopConfiguration)

        self._block_size = self._file_system.getDefaultBlockSize()

    def _clear_directory(self, path_to_dir: str):
        if self._file_system.exists(self._Path(path_to_dir)):
            self._file_system.delete(self._Path(path_to_dir), True)
            logger.debug("Delete " + path_to_dir)

    @staticmethod
    def _filter_directories(path: str, min_date='', max_date=''):
        suit = min_date <= path[-10:] and path != "_SUCCESS" and path[0] != "."
        if max_date != '':
            suit = path[-10:] <= max_date and suit

        return suit

    def delete_all_files_in_directory(self, path_to_dir: str):
        file_list = []
        logger.debug(f"start to delete path =  {path_to_dir}")
        if self._file_system.exists(self._Path(path_to_dir)):
            logger.debug("this path has subdirs")
            file_list = [str(file_status.getPath()) for file_status in
                         self._file_system.listStatus(self._Path(str(path_to_dir)))]
        logger.debug("Delete " + str(file_list))
        for file_for_del in file_list:
            if self._file_system.exists(self._Path(file_for_del)):
                self._file_system.delete(self._Path(file_for_del), True)
                # logger.debug("Delete " + file_for_del)
        return None

    def delete_partitions(self, source_dir: str, from_date='', to_date=''):
        """
        Удаляет устаревшие партиции, находящиеся в переданной директории

        Args:
            source_dir: путь до директории с партициями
            from_date: дата в конце имени директории вида '%Y-%m-%d', начиная с которой партиции удаляются
            to_date: дата в конце имени директории вида '%Y-%m-%d', до которой партиции удаляются
        """
        file_list = []
        if self._file_system.exists(self._Path(source_dir)):
            file_list = [str(file_status.getPath()) for file_status in
                         self._file_system.listStatus(self._Path(str(source_dir)))
                         if file_status.isDirectory() and self._filter_directories(str(file_status.getPath()),
                                                                                   min_date=from_date,
                                                                                   max_date=to_date)]
            logger.info("Delete in dir: " + str(source_dir))
            logger.info("Delete from date: " + str(from_date) + " Delete to date: " + str(to_date))
            logger.info("File_list: " + str(file_list))

            for file in file_list:
                self._clear_directory(file)
        else:
            logger.info("file_list is empty!: " + str(file_list))
            logger.info("Not found dir: " + str(source_dir))

    def repart(self, source_dir: str):
        """
        Укрупняет партиции, находящиеся в переданной директории, или саму директорию, если она является партицией.
        Все вложенные директории (только по переданному пути) рассматриваются как партции.
        Метод ловит исключения при чтении каждой отдельной партиции.

        Args:
            source_dir: путь до директории с партициями
        """

        logger.debug("search new partitions dirs")
        dir_list = self.get_directories_list(source_dir)

        if len(dir_list) != 0:
            logger.debug("File list " + str(dir_list))
            res_list = []
            with ThreadPoolExecutor(len(dir_list)) as executor:
                future_list = []
                for file in dir_list:
                    future = executor.submit(self.repart_one_partition, file)
                    future_list.append(future)
                    # TODO проверить разницу между старой реализацией и этой и так же проверить,
                    #  что submit выполняется без доп внутренней функции
                    res_list.append(future.result())
                # for future in future_list:
                #     res_list.append(future.result())
            if not 1 in res_list:
                raise ValueError("Some executor return ERROR result when do function repart")
            else:
                logger.info("SUCCESS repart for" + str(source_dir))
        else:
            logger.info("Not found files for repart")

        tmp_dir = source_dir + "_temp"
        logger.info("delete " + tmp_dir)
        self._file_system.delete(self._Path(tmp_dir), True)

    def repart_one_partition(self, source_dir: str, tmp_dir: str = None):
        """
        Укрупняет паркетники, находящиеся в переданной директории (не рассматривая вложенные директории).
        Метод ловит исключения при чтении parquet файлов и выводит их в логи.

        Args:
            source_dir: путь до директории с parquet файлами
            tmp_dir: путь до директории для временных файлов
        """
        logger.info("Укрупняем файлы в партции: " + source_dir + "\n")
        if tmp_dir is None:
            tmp_dir = source_dir + "_temp"
            self._clear_directory(tmp_dir)

        # Находим размер в байтах
        size_in_bytes = self._file_system.getContentSummary(self._Path(source_dir)).getLength()
        if size_in_bytes == 0:
            logger.info("Current directory is empty")
            return None

        logger.debug("Size in bytes " + str(size_in_bytes))
        # Находим число блоков для репартиционирования
        repart_factor = (size_in_bytes // self._block_size) + 1

        logger.debug("Start reading from source")
        df = self._spark.read.parquet(source_dir)

        logger.debug("Start repartition and save in tmp")
        df.repartition(repart_factor).write.mode("append").parquet(tmp_dir)

        logger.debug("delete old partition dir")
        self._clear_directory(source_dir)

        logger.debug("Move " + tmp_dir + " --> " + source_dir)
        self._file_system.rename(self._Path(tmp_dir), self._Path(source_dir))
        return 1

    def get_directories_list(self, location: str, compare_opreation: operator = operator.le) -> list:
        """
        Функция проверяет присутствие в указанном location файлов .parquet и .orc
        Если в location есть директории, то рекурсивно вызывает сама себя и в них ищет файлы.

        Возвращает лист в директориями, которые содержат файлы с данными.
        Если таблица не партицирована - возвращает location таблицы

        Args:
            location: путь до таблицы
        """
        dir_list = []
        if not self._file_system.listFiles(self._Path(location), True).hasNext():
            logger.info("Table directory {0} is empty.".format(location))
            return dir_list
        else:
            for file_status in self._file_system.listStatus((self._Path(location))):
                get_date = datetime.fromtimestamp(file_status.getModificationTime() / 1000).strftime(
                    '%Y-%m-%d %H:%M:%S')
                if compare_opreation(self._start_wf_time, get_date):
                    file_path = file_status.getPath()
                    logger.debug(
                        f"{self._start_wf_time} and {get_date} fulfill the condition for path = {str(file_path).split('/')[-1]}")
                    if self._file_system.getFileStatus(file_path).isFile():
                        dir_list.append(str(self._Path(str(file_status.getPath())).getParent()))
                        break
                    elif self._file_system.getFileStatus(file_path).isDirectory():
                        logger.info(f"this path is directory {str(file_path).split('/')[-1]}")
                        dir_list.extend(self.get_directories_list(str(file_path), compare_opreation))
        return dir_list

    @staticmethod
    def extract_partition_date(row: str, partition_name: str) -> str:
        """
        Излвекает дату указанной партиции
        Args:
            row: входной путь
            partition_name: название строки
        """
        partition_value = re.findall(fr'{partition_name}=([^/]*)', row)[0]
        logger.debug(partition_value)
        return partition_value

    def delete_old_partitions(self, location: str, partition_name: str, last_days: int = 0, last_months: int = 0,
                              last_years: int = 0) -> None:
        """
        # TODO изменить delete partitions и просто удалять партиции до определённой даты. Тогда этот метод будет не нужным!
        # TODO упростить логику на 1 строку.
        Удаляет устаревшие партиции, на основе маски для парсинга:
        План:
        1) находим локейшн таргет таблицы
        2) проходим по всем партициям верхнего уровня (либо нижнего, пока что не понял)
        3) Делим строку
        """

        curr_date = datetime.strptime(self._start_wf_time, '%Y-%m-%d %H:%M:%S')
        last_date = curr_date - relativedelta.relativedelta(days=last_days, months=last_months, years=last_years)
        logger.debug("convert to string again")

        last_date = last_date.strftime('%Y-%m-%d %H:%M:%S')
        logger.debug(f"last_date  = {last_date}")

        partition_list = self.get_directories_list(location=location, compare_opreation=operator.ge)
        logger.debug(f"all dirs = {len(partition_list)}")

        old_partitions_list = list(
            filter(lambda x: self.extract_partition_date(x, partition_name) < last_date, partition_list))
        logger.debug(f"filtered list = {old_partitions_list}")

        logger.debug("start to delete partitions")
        for loc in old_partitions_list:
            logger.debug(f"loc = {loc}")
            self._clear_directory(loc)
        return None

# TODO переписать весь файл. Добавить реализацию от Димы