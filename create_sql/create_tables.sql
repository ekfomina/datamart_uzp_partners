DROP TABLE IF EXISTS custom_salesntwrk_ul_profile.uzp_partners_features;
CREATE EXTERNAL TABLE IF NOT EXISTS custom_salesntwrk_ul_profile.uzp_partners_features (
    ctl_loading                     int         comment 'Идентификатор потока загрузки',
    ctl_validfrom                   string      comment 'Дата ETL процесса',
    inn                             string      comment 'ИНН ЮЛ',
    okved_cd                        string    comment 'Код ОКВЭД',
    segment                         string      comment 'Сегмент',
    sector                          string          comment 'Сектор',
    kopf_nm                         string      comment 'Тип организации',
    days_from_registration          smallint comment 'Период с момента регистрации, дней',
    op_tech_other_bank_cnt          smallint            comment 'Количество всех платежей, связанных со счетами из других банков (расчетный счет у одной стороны)',
    balance_rs                      float       comment 'Остаток на конец месяца по расчётным счетам',
    out_tech_our_bank_contragents   smallint     comment 'Количество контрагентов во всех исходящих платежах со счетов из сбербанка (расчетный счет у одной стороны)',
    in_tech_our_bank_contragents    smallint       comment 'Количество контрагентов во всех входящих платежах на счета из сбербанка (расчетный счет у одной стороны)',
    in_tech_our_bank_cnt            smallint comment 'Количество всех входящих платежей на счетах из сбербанка (расчетный счет у одной стороны)',
    op_tech_our_bank_amt            float        comment 'Сумма всех платежей, связанных со счетами из сбербанка (расчетный счет у одной стороны)',
    op_pays_our_bank_cnt            smallint              comment 'Количество всех платежей, связанных со счетами из сбербанка (только транзакции между расчетными счетами)',
    out_pays_our_bank_cnt           smallint             comment 'Количество всех исходящих платежей со счетов из сбербанка (только транзакции между расчетными счетами)',
    in_pays_our_bank_contragents    smallint    comment 'Количество контрагентов во всех входящих платежах со счетов из других банков (только транзакции между расчетными счетами)',
    op_pays_our_bank_amt            float       comment 'Сумма всех платежей, связанных со счетами из сбербанка (только транзакции между расчетными счетами)'
)
PARTITIONED BY (report_dt       string      comment          'Дата формирования витрины')
STORED AS PARQUET
LOCATION '/data/custom/salesntwrk/ul_profile/pa/uzp_partners_features';
MSCK REPAIR TABLE custom_salesntwrk_ul_profile.uzp_partners_features