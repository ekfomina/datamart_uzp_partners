import json
import logging
import logging.config
import os
import re
import sys
import traceback
from datetime import datetime
from logging import Formatter
from logging import Handler

import requests
import yaml
from pyspark import SparkConf
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def yaml_with_env_var():
    """
    Позволяет в YAML файлах переменные окружения использовать

    те если
    в yaml файле есть конструрция var_name: ${CTL_URL}
    и есть переменная огружиеня CTL_URL="localhost"

    то при парсинге yaml файла получим
    {var_name: "localhost"}
    """
    path_matcher = re.compile(r'\$\{([^}^{]+)\}')

    # как сделать лучше, я не нашел/придумал
    def path_constructor(loader, node):
        ''' Extract the matched value, expand env variable, and replace the match '''
        value = node.value

        match = path_matcher.match(value)

        env_var = match.group(1)

        return os.environ.get(env_var) + value[match.end():]
    # ВНИМАНИЕ расширяем функционал yaml пакета
    yaml.add_implicit_resolver('!path', path_matcher, None, yaml.SafeLoader)
    yaml.add_constructor('!path', path_constructor, yaml.SafeLoader)

    return None


def add_logging_level(level_name, level_num, method_name=None):
    """
    Comprehensively adds a new logging level to the `logging` module and the
    currently configured logging class.

    `levelName` becomes an attribute of the `logging` module with the value
    `levelNum`. `methodName` becomes a convenience method for both `logging`
    itself and the class returned by `logging.getLoggerClass()` (usually just
    `logging.Logger`). If `methodName` is not specified, `levelName.lower()` is
    used.

    To avoid accidental clobberings of existing attributes, this method will
    raise an `AttributeError` if the level name is already an attribute of the
    `logging` module or if the method name is already present

    Example
    -------
    >>> add_logging_level('TRACE', logging.DEBUG - 5)
    >>> logging.getLogger(__name__).setLevel("TRACE")
    >>> logging.getLogger(__name__).trace('that worked')
    >>> logging.trace('so did this')
    >>> logging.TRACE
    5

    """
    if not method_name:
        method_name = level_name.lower()

    if hasattr(logging, level_name):
       raise AttributeError('{} already defined in logging module'.format(level_name))
    if hasattr(logging, method_name):
       raise AttributeError('{} already defined in logging module'.format(method_name))
    if hasattr(logging.getLoggerClass(), method_name):
       raise AttributeError('{} already defined in logger class'.format(method_name))

    # This method was inspired by the answers to Stack Overflow post
    # http://stackoverflow.com/q/2183233/2988730, especially
    # http://stackoverflow.com/a/13638084/2988730
    def logForLevel(self, message, *args, **kwargs):
        if self.isEnabledFor(level_num):
            self._log(level_num, message, args, **kwargs)
    def logToRoot(message, *args, **kwargs):
        logging.log(level_num, message, *args, **kwargs)

    logging.addLevelName(level_num, level_name)
    setattr(logging, level_name, level_num)
    setattr(logging.getLoggerClass(), method_name, logForLevel)
    setattr(logging, method_name, logToRoot)

    return None


class HDFSDataStream:
    def __init__(self, spark, path_to_hdfs):
        self._spark = spark
        self._path_to_hdfs = path_to_hdfs
        self._data_stream = None

    def write(self, message: str):
        self._data_stream.write(message)

    def __enter__(self):
        """
        Здесь инициализирукется объект, который позволит работать с файлом в hdfs

        изпульзуя синтаксис, реализующий контекстный менеджер

        Аналогией с обычной файловой системой будем метод open(<file_path>)
        который открывает файл на чтение/запись и позволяет с ним работать

        """
        FileSystem = self._spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem
        hadoopConfiguration = self._spark.sparkContext._jsc.hadoopConfiguration()
        hadoopFs = FileSystem.get(hadoopConfiguration)

        Path = self._spark.sparkContext._jvm.org.apache.hadoop.fs.Path
        filePath = Path(self._path_to_hdfs)

        if hadoopFs.exists(filePath):
            self._data_stream = hadoopFs.append(filePath)
        else:
            self._data_stream = hadoopFs.create(filePath)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._data_stream.close()


class HDFSHandler(Handler):
    def __init__(self, hdfs_file_path: str):
        super().__init__()
        self.hdfs_file_path = hdfs_file_path
        self.spark = self._get_spark_session(app_name="hdfs_logger")

    def emit(self, record):
        formatted_record = self.format(record)
        formatted_record += "\n"
        formatted_record_encoded = formatted_record.encode("utf-8")

        with HDFSDataStream(self.spark, self.hdfs_file_path) as data_steam:
            data_steam.write(formatted_record_encoded)

    @staticmethod
    def _get_spark_session(app_name: str) -> SparkSession:
        conf = SparkConf().setAppName(app_name)
        spark = SparkSession.builder.config(conf=conf).getOrCreate()
        return spark


class SberLogstashHandler(Handler):

    def __init__(self, url):
        super().__init__()
        self.url = url

        # нужен, чтобы в случае ошибки с логированием в logstash написать об этом
        # тк потом именно логер urllib3 и перенаправится в другие хандлеры
        self.logger = logging.getLogger("urllib3")

    def emit(self, record):
        formatted_record = self.format(record)

        header = {"Content-Type": "application/json"}

        try:
            response = requests.post(url=self.url,
                                     data=formatted_record,
                                     headers=header,
                                     timeout=3)
            if not response.ok:
                sc = response.status_code
                rs = response.reason
                self.logger.warning("logger response status code {sc}: {rs}".format(sc=sc, rs=rs))

        except Exception as e:
            self.logger.warning(str(e))

        return None


class SberLogstashFormatter(Formatter):
    def __init__(self, app_id: str):
        super().__init__()
        self.app_id = str(app_id)

    def format(self, record):
        data = {"app_id": self.app_id,
                "timestamp": int(record.created),
                "type_id": "Tech",
                "subtype_id": record.levelname,
                "value": record.levelno,
                "message": record.msg,
                "name": record.name
                }

        return json.dumps(data)


class CTLogHandler(Handler):
    def __init__(self, url):
        super().__init__()
        self.url = url

    def emit(self, record):
        formatted_record = self.format(record)

        header = {"Content-Type": "application/json"}

        try:
            response = requests.put(url=self.url,
                                    data=formatted_record,
                                    headers=header,
                                    timeout=3)
            if not response.ok:
                sc = response.status_code
                rs = response.reason
                logging.warning("logger response status code {sc}: {rs}".format(sc=sc, rs=rs))

        except Exception as e:
            logging.warning(str(e))

        return None


class CTLLogFormatter(Formatter):
    def __init__(self, loading_id, ctl_date_fmt='%Y-%m-%d %H:%M:%S'):
        super().__init__()
        self.loading_id = loading_id
        self.ctl_date_fmt = ctl_date_fmt

    def format(self, record: logging.LogRecord) -> str:
        cur_date = datetime.now()
        cur_date_str = cur_date.strftime(self.ctl_date_fmt)

        if record.levelno >= logging.ERROR:
            status = "ERROR"
        else:
            status = "RUNNING"

        data = {"loading_id": int(self.loading_id),
                "effective_from": cur_date_str,
                "status": status,
                "log": record.msg}

        return json.dumps(data)


class CTLogFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if record.levelno > logging.INFO:
            return True
        else:
            return False


def setup_logger(path_to_conf) -> None:
    """
    Чтение настроек логгера из конфига
    и настройка самого логера и все что для этого требуется

    Returns: None
    """
    yaml_with_env_var()

    add_logging_level(level_name='CTL_INFO', level_num=logging.INFO + 5)

    with open(path_to_conf) as f:
        config = yaml.safe_load(f)

    logging.config.dictConfig(config)

    # перехватываем stacktrace логгером
    sys.excepthook = exception_hook


def exception_hook(exc_type, exc_value, exc_traceback):
    """
    Нужно, чтобы перехватывать traceback логером
    """

    fmt_stacktrace = '\n'.join(traceback.format_tb(exc_traceback))

    logging.error(msg="{exc_value}\n{fmt_stacktrace}".format(exc_value=exc_value, fmt_stacktrace=fmt_stacktrace),
                  exc_info=(exc_type, exc_value, exc_traceback))

    return None
