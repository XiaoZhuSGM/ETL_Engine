from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from flask import request
from config.config import config
import os

INSERTSQL = """insert into chain_sales_target_{source_id}
                (source_id,cmid,target_date,foreign_store_id,store_show_code,store_name,target_sales,target_gross_profit,
                foreign_category_lv1,foreign_category_lv2,foreign_category_lv3,foreign_category_lv4,foreign_category_lv5)
                values{values}"""
SELECTSTORE = """select source_id,foreign_store_id,store_name from chain_store where cmid = {cmid} and show_code = '{show_code}'"""
DELETESALES = """delete from chain_sales_target_{source_id} where source_id='{source_id}' and target_date='{date1}' and foreign_store_id in ({deletes})"""
INSERTVALUE = """('{source_id}',{cmid},'{date1}','{store_id}','{show_code}','{store_name}',{target_sales},{target_gross_profit},'','','','','')"""

setting = config[os.getenv("ETL_ENVIREMENT", "dev")]
REDSHIFT_URL = setting.REDSHIFT_URL


class LoadSalestargetServices:

    def load_sales_target(self):
        """
        后端调用此接口，导入销售目标表到redshift
        :return:
        """
        engine = create_engine(REDSHIFT_URL)

        data = request.get_json()
        cmid = data.get("cmid")
        date1 = data.get("date")
        target_list = data.get("data")

        value_list = []
        delete_list = []
        with engine.connect() as conn:
            for target in target_list:
                select_sql = SELECTSTORE.format(cmid=cmid, show_code=target.get("showcode"))
                result = conn.execute(select_sql).first()
                if not result:
                    continue
                source_id = result[0]
                foreign_store_id = result[1]
                store_name = result[2]
                show_code = target.get("showcode")
                target_sales = target.get("target_sales")
                target_gross_profit = target.get("target_gross_profit")

                delete = f"'{foreign_store_id}'"
                delete_list.append(delete)

                value = INSERTVALUE.format(
                    cmid=cmid, store_id=foreign_store_id, show_code=show_code, store_name=store_name,
                    target_sales=target_sales, date1=date1, target_gross_profit=target_gross_profit,
                    source_id=source_id)
                value_list.append(value)

            deletes = ",".join(delete_list)
            delete_sales_sql = DELETESALES.format(source_id=source_id, date1=date1, deletes=deletes)

            values = ",".join(value_list)
            insert_sql = INSERTSQL.format(source_id=source_id, values=values)
            with conn.begin():
                conn.execute(delete_sales_sql)
                conn.execute(insert_sql)
