INSERT OVERWRITE TABLE {target_name} PARTITION (report_dt)
SELECT
  {ctl_loading} as ctl_loading
, "{ctl_validfrom}" as ctl_validfrom
, ul.inn
, mon.okved_cd
, mon.segment
, mon.sector
, mon.kopf_nm
, ul.days_from_registration
, ul.op_tech_other_bank_cnt
, ul.balance_rs
, ul.out_tech_our_bank_contragents
, ul.in_tech_our_bank_contragents
, ul.in_tech_our_bank_cnt
, ul.op_tech_our_bank_amt
, ul.op_pays_our_bank_cnt
, ul.out_pays_our_bank_cnt
, ul.in_pays_our_bank_contragents
, ul.op_pays_our_bank_amt
, ul.report_dt
from {ul_profile} ul
left join {mon_basis_corporate} mon on ul.inn = mon.inn
where ul.report_dt = "{default_date}"