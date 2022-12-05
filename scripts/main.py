import logging
import os
import traceback
from argparse import ArgumentParser
from datetime import datetime

from utils import args_parser
from utils import etl_utils as ETLU
from utils.logger_utils import setup_logger
from utils.partition_utils import PartitionController
from utils.etl_utils import EtlMethods, KpdReport
import re

logger = logging.getLogger(__name__)
# Константы, которые не относятся к ETL вычислениям
CTL_VALIDFROM = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
LOADING_ID = int(os.environ.get('LOADING_ID'))
CTL_URL = os.environ.get('CTL')
CTL_ENTITY_ID = int(os.environ.get('CTL_ENTITY_ID'))

if __name__ == "__main__":
    """
    1. Инициализируем: Spark, Argparse, Logger. Настраиваем ETL переменные
    """
    parser = ArgumentParser()
    args = args_parser.parse_arguments(parser)
    spark = ETLU.get_spark(os.environ.get("WF_NAME") + str(LOADING_ID))
    partition = PartitionController(spark, CTL_VALIDFROM)
    setup_logger("scripts/logger.yaml")
    logger.ctl_info("Log in HDFS: {}".format(os.environ.get('PATH_TO_LOG_IN_HDFS')))

    # Выводим все ETL константы
    etl_vars = args_parser.prepare_etl_arguments(args.etl_vars)
    # TODO подумать, как избавиться от пересоздания таблиц до архивной загрузки
    # TODO переписать методы, чтобы не нужно было хранить location таблицы
    # TODO написать pytest-ы
    ETLU.recreate_table(spark, path_to_query='create_sql/*.sql', **etl_vars)
    etl_vars["table_location"] = EtlMethods.get_hdfs_location(spark, etl_vars["target_name"])

    kpd_report = KpdReport(spark, **etl_vars)
    logger.ctl_info(str(etl_vars))

    """
    2. FORCE LOAD & Recreate tables
    """
    # Сейчас без этого никак, т.к. при первом запуске обязательно выполнять скрипт в архивном режиме
    # Иначе скрипт будет искать последнюю дату загрузки таргет таблицы и упадёт
    if args.force_load:
        logger.ctl_info("Force load is True. Start to delete all partitions")
        partition.delete_partitions(source_dir=etl_vars["table_location"])
    ETLU.recreate_table(spark, path_to_query='create_sql/*.sql', **etl_vars)

    """
    3. Выполняем все sql скрипты
    """
    # Если у нас не пустая дата и не архивная загрузка - находим последнее актуальное значение
    if etl_vars["default_date"] and not args.force_load:
        etl_vars["default_date"] = EtlMethods.get_max_value(spark, etl_vars["target_db"], etl_vars["target_table"],
                                                            etl_vars["partition_name"])

    # Находим последний выполненный шаг
    last_computed_step = kpd_report.get_last_computed_step(args.recovery_mode)
    logger.info(f"Last step: {last_computed_step}")

    logger.ctl_info("Execute create_sql queries and insert data")
    files = ETLU.get_grouped_sql_files_by_pattern_with_step(path_to_scanning="scripts/sql/",
                                                            pattern="step*")
    files.sort()
    last_step_in_general = len(files)
    for step, sql_list in files:
        if step <= last_computed_step:
            logger.ctl_info(f"step {step} has been skipped")
            continue

        try:
            logger.info('Start execute: step ' + str(step))
            ETLU.parallel_execution_queries_from_list(spark, sql_list,
                                                      ctl_loading=LOADING_ID,
                                                      ctl_validfrom=CTL_VALIDFROM,
                                                      **etl_vars)
            last_computed_step = step
        except:
            logger.error(traceback.format_exc())
            break

    logger.info('----------------------')
    logger.ctl_info(f"SUCCESS: main create_sql finished on step {last_computed_step} out of {last_step_in_general}")

    """
    4. Записываем результаты в kpd report
    """
    previous_count_of_records = kpd_report.get_previous_count_of_records()
    current_count_of_records = EtlMethods.cnt_rows(spark, etl_vars['target_db'], etl_vars['target_table'])

    change = int(args.force_load or previous_count_of_records != current_count_of_records)
    logger.ctl_info(f"Change = {change}. Num rows = {current_count_of_records}")

    partitions = EtlMethods.get_last_partition(spark, etl_vars["target_db"], etl_vars["target_table"])

    # Важно! Указывать название переменных в соответствии с названием столбца
    EtlMethods.insert_single_row_data(spark, etl_vars["target_db"], etl_vars["kpd_table_name"],
                                      ctl_loading=LOADING_ID,
                                      ctl_validfrom=CTL_VALIDFROM,
                                      partitions=partitions,
                                      is_force_load=args.force_load,
                                      last_success_step=last_computed_step,
                                      last_step_in_general=last_step_in_general,
                                      current_count=current_count_of_records,
                                      is_change=change)

    logger.ctl_info("kpd report has been updated!")

    # Checking the success of operation
    assert last_computed_step == last_step_in_general, "Computation of datamart failed"

    """
    4.1 Делаем репартиционирование файлов по указанной дирректории
    """
    logger.ctl_info("Start repartition")
    partition = PartitionController(spark, CTL_VALIDFROM)
    partition.repart(etl_vars["table_location"])

    logger.ctl_info("repartition completed")

    logger.ctl_info("delete old partitions")
    partition.delete_old_partitions(location=etl_vars["table_location"], partition_name=etl_vars["partition_name"],
                                    last_days=etl_vars["lifetime_in_days"], last_months=etl_vars["lifetime_in_months"], last_years=etl_vars["lifetime_in_years"])
    logger.ctl_info("partitions have been deleted")

    """
    5. Пишем статистики
    """
    logger.ctl_info("upload ctl statistics")

    ctl_url_stat = CTL_URL + "/v1/api/statval/m"
    dt_now = str(datetime.now().date())
    # TODO УБРАТЬ КОСТЫЛЬНУЮ МАСКУ ПО ДАТЕ, ЗАМЕНИТЬ НА МАСКУ ИЗ МЕТОДА DELETE OLD PARTITIONS
    mask = "\d{4}-\d{2}-\d{2}"
    report_dt = re.search(mask, partitions)[0]

    ctl_stat = ETLU.CTLStatistics(ctl_url=ctl_url_stat,
                                  loading_id=LOADING_ID,
                                  ctl_entity_id=CTL_ENTITY_ID)

    ctl_stat.upload_statistic(stat_id=1, stat_value=dt_now)

    ctl_stat.upload_statistic(stat_id=2, stat_value=str(change))

    ctl_stat.upload_statistic(stat_id=5, stat_value=report_dt)

    ctl_stat.upload_statistic(stat_id=11, stat_value=CTL_VALIDFROM)

    ctl_stat.upload_statistic(stat_id=15, stat_value=str(spark._sc.applicationId))

    logger.info("end_python")

    # нужно закрывать именно просле последнего лог мессаджа либо вообще не закрывтать
    # тк если используется хандлер в hdfs то после закрытия он не отработает
    spark.stop()
