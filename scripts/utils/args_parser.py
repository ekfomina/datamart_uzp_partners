"""
В данный модуль вынесен функционал, связанный с обработкой аргументов, переданных скрипту
"""
import json
from collections import defaultdict


def json_dict(input_data: str) -> dict:
    """
    Args:
        input_data: входная строка формата json с одинарными кавычками

    Returns:
        Python словарь
    """
    return json.loads(input_data.replace("'", '"'))


def parse_arguments(parser):
    parser.add_argument("--force_load",
                        dest="force_load",
                        type=int,
                        help="flag of archive loading",
                        default=0)
    parser.add_argument("--recovery_mode",
                        dest="recovery_mode",
                        type=int,
                        help="flag of recovery loading with using computed stage table",
                        default=0)
    # Переменные, которые добавляются опционально.
    # Название таблицы, значение поля партицирования, значение поля партицирования
    parser.add_argument("--etl_vars",
                        dest="etl_vars",
                        type=json_dict,
                        required=True)
    known_args, unknown_args = parser.parse_known_args()

    return known_args


def prepare_etl_arguments(etl_dict: dict) -> defaultdict:
    """
    Обрабатывает входной словарь, доополняя его новыми аргументами, созданными из уже имеющихся.
    + Возвращает defaultdict, чтобы не прописывать все аргументы явно, если они необходимы.
    Args:
        etl_dict - Словарь входных данных
    Requires:
        Словарь должен иметь обязательное поле - target_name в формате target_db.target_table
    Returns:
        Обработанный дефолтный словарь входных данных
    """
    # TODO ДОПИСАТЬ ПРОВЕРКИ И ДЕФОЛТНЫЕ ЗНАЧЕНИЯ
    assert "target_name" in etl_dict, f"Dict {etl_dict} has No key target_name!"
    etl_dict["target_db"], etl_dict["target_table"] = etl_dict["target_name"].split(".")
    etl_dict["kpd_table_name"] = "kpd_report_" + etl_dict["target_table"]
    etl_dict["target_path"] = etl_dict["target_db"].replace("custom_salesntwrk_", "")
    return defaultdict(str, etl_dict)
