from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from flask import request
# REDSHIFT_URL = "postgresql://cmb_chainstore:yuSpUh_2aX!@cmb-chainstore.cdl8ar96w1hm.rds.cn-north-1.amazonaws.com.cn/cmb_chainstore"
***REMOVED***
INSERTSQL = """insert into chain_sales_target_{source_id}
                (source_id,cmid,target_date,foreign_store_id,store_show_code,store_name,target_sales,target_gross_profit,
                foreign_category_lv1,foreign_category_lv2,foreign_category_lv3,foreign_category_lv4,foreign_category_lv5)
                values('{source_id}',{cmid},'{date1}','{store_id}','{show_code}','{store_name}',{target_sales},{target_gross_profit},'','','','','')"""
SELECTSTORE = """select source_id,foreign_store_id,store_name from chain_store where cmid = {cmid} and show_code = '{show_code}'"""
SELECTSALES = """select * from chain_sales_target_{source_id} where source_id='{source_id}' and foreign_store_id = '{foreign_store_id}' and target_date='{date1}'"""
UPDATESQL = """update chain_sales_target_{source_id} set target_sales={target_sales},target_gross_profit={target_gross_profit} where source_id='{source_id}' and foreign_store_id = '{foreign_store_id}' and target_date='{date1}'"""


class ParameterError(Exception):
    def __str__(self):
        return "parameter error"


class LoadSalestargetServices:

    def load_sales_target(self):
        """
        后端调用此接口，导入销售目标表到redshift
        :return:
        """
        data = request.get_json()
        cmid = data.get("cmid")
        date1 = data.get("date")
        target_list = data.get("data")

        if not all([cmid, date1, target_list]):
            raise ParameterError()

        engine = create_engine(TEST_REDSHIFT_URL)
        Session = sessionmaker(bind=engine)
        session = Session()

        for target in target_list:
            select_sql = SELECTSTORE.format(cmid=cmid, show_code=target.get("foreign_store_id"))
            result = session.execute(select_sql).first()
            if not result:
                continue
            source_id = result[0]
            foreign_store_id = result[1]
            store_name = result[2]
            show_code = target.get("foreign_store_id")
            target_sales = int(target.get("target_sales"))
            target_gross_profit = int(target.get("target_gross_profit"))
            select_sales_sql = SELECTSALES.format(
                source_id=source_id, foreign_store_id=foreign_store_id, date1=date1)
            result = session.execute(select_sales_sql).first()

            if not result:
                insert_sql = INSERTSQL.format(
                    cmid=cmid, store_id=foreign_store_id, show_code=show_code, store_name=store_name,
                    target_sales=target_sales, date1=date1, target_gross_profit=target_gross_profit,
                    source_id=source_id)

                session.execute(insert_sql)
                continue

            updete_sql = UPDATESQL.format(
                source_id=source_id, target_sales=target_sales, target_gross_profit=target_gross_profit,
                foreign_store_id=foreign_store_id, date1=date1)
            session.execute(updete_sql)
        try:
            session.commit()
        except Exception as e:
            session.rollback()
            raise e

