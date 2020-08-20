# Code_Python_190000_wj_tools
Python 工具包

**使用 py_tools 的项目**
gansu_payback  => py_tools
    ssh 10.91.1.19 "/app/code/venv/venv1_hive/bin/python3 /app/code/payback/gansu_payback.py"

**outsourcing_insert**  => py_tools
    ssh 10.91.1.19 "/app/code/venv/venv1_hive/bin/python3 /app/code/outsourcing/outsourcing.py"

**payback**  => py_tools
    ssh 10.91.1.19 "/app/code/venv/venv1_hive/bin/python3 /app/code/payback/payback.py"

**使用 wj_tools 的项目**
**qsdai**  => wj_tools
    /app/code/venv/venv1_hive/bin/python3 /app/code/qsdai/qsdai.py



cp -u -R -p /home/data/SYB  /backup/home/data/SYB




(**taxloan_collection**)  => (内含)wj_tools
collection_report_taxloan
    ssh 10.91.1.19 "/app/code/venv/venv1_hive/bin/python3 /app/code/collection/collection_taxloan.py"
collection_report_taxloan_weiwai
    ssh 10.91.1.19 "/app/code/venv/venv1_hive/bin/python3 /app/code/collection/collection_taxloan_weiwai_rev2.py"
    ftp://10.91.1.18

**taxloan_invoice_monitor_report**  => (内含)wj_tools
    ssh 10.91.1.19 "/app/code/venv/venv1_hive/bin/python3 /app/code/deduct/taxloan_invoice_monitor_report.py"

**taxloan_line_items_weekly_report**  => (内含)wj_tools
	ssh 10.91.1.19 "/app/code/venv/venv1_hive/bin/python3 /app/code/taxloan/taxloan_new_old_taxno.py"
    ssh 10.91.1.19 "/app/code/venv/venv1_hive/bin/python3 /app/code/taxloan/taxloan_line_items.py"

Project **tlb_daily**  Job report_tlb_daily_by_branch_py  => (内含)wj_tools
    ssh 10.91.1.19 "/app/code/venv/venv1_hive/bin/python3 /app/code/deduct/report_tlb_daily_by_branch.py"

Project tlb_line_items  Flow ftp_tlb_line_items  => (内含)wj_tools
    ssh 10.91.1.19 "/app/code/venv/venv1_hive/bin/python3 /app/code/deduct/loan_credit_apply_line_items.py"
    ssh 10.91.1.19 "/app/code/venv/venv1_hive/bin/python3 /app/code/deduct/risk_mgr_info_rig_line_items.py"
    ssh 10.91.1.19 "/app/code/venv/venv1_hive/bin/python3 /app/code/deduct/loan_use_apply_line_items.py"

Project **tlb_monthly**  Job report_tlb_monthly_by_branch_py  => (内含)wj_tools
    ssh 10.91.1.19 "/app/code/venv/venv1_hive/bin/python3 /app/code/deduct/report_tlb_monthly_by_branch.py"

Project **tlb_weekly**  Job report_tlb_weekly_by_branch_py   => (内含)wj_tools
    ssh 10.91.1.19 "/app/code/venv/venv1_hive/bin/python3 /app/code/deduct/report_tlb_weekly_by_branch.py"

