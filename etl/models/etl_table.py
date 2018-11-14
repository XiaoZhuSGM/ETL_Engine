from sqlalchemy import VARCHAR, REAL, Integer, DateTime, String, Boolean, Column
from sqlalchemy.dialects.postgresql import JSONB
from etl.etl import db
from .base import CRUDMixin
from sqlalchemy.orm import relationship


class ExtErpEnterprise(CRUDMixin, db.Model):
    name = Column(VARCHAR(100))
    version = Column(VARCHAR(50))
    remark = Column(VARCHAR(1000))


class ExtChainStoreOnline(CRUDMixin, db.Model):
    source_id = Column(String(15))
    cmid = Column(Integer)
    company_name = Column(VARCHAR(100))


class ExtStoreDetail(CRUDMixin, db.Model):
    source_id = Column(String(15))
    cmid = Column(Integer)
    store_id = Column(String(50))
    store_name = Column(VARCHAR(100))
    total_cost = Column(REAL)
    total_sale = Column(REAL)
    ext_date = Column(DateTime)


class ExtLogInfo(CRUDMixin, db.Model):
    source_id = Column(String(15))
    cmid = Column(Integer)
    task_type = Column(Integer)
    table_name = Column(String(512))
    record_num = Column(Integer)
    start_time = Column(DateTime)
    end_time = Column(DateTime)
    cost_time = Column(Integer)
    extract_date = Column(DateTime)
    result = Column(Integer)
    remark = Column(VARCHAR(1000))


class ExtCleanInfo(CRUDMixin, db.Model):
    """
    合成目标表所需要信息
    like    {
                "t_sl_master": ['fbrh_no', 'fflow_no', 'ftrade_date', 'fcr_time', 'fsell_way'],
                "t_sl_detail": ['fprice', 'fpack_qty', 'famt', 'fflow_no', 'fitem_subno', 'fitem_id'],
                "t_br_master": ['fbrh_name', 'fbrh_no'],
                "t_bi_master": ['fitem_id', 'fitem_subno', 'fitem_name', 'funit_no', 'fitem_clsno'],
                "t_bc_master": ['fitem_clsno', 'fitem_clsname', 'fprt_no'],
                "t_bi_barcode": ['funit_qty', 'fitem_id', 'fitem_subno']
            }
    like    {
                "t_sl_master": {"fbrh_no": "str"}, "t_br_master": {"fbrh_no": "str"},
                "t_bi_master": {"fitem_clsno": "str"},
                "t_bc_master": {"fitem_clsno": "str", "fprt_no": "str"}
            }
    """

    source_id = Column(String(15))
    origin_table = Column(JSONB, comment="合成目标表需要的原始表何所需要的字段")
    covert_str = Column(JSONB, comment="需要格式转换的字段，防止pandas家在丢失数据位")
    target_table = Column(
        VARCHAR(50), comment="目标表，譬如goodsflow_32yyyyyyyyyyyyy,chain_goods等"
    )
    deleted = Column(Boolean)

    datasource = relationship(
        "ExtDatasource",
        primaryjoin="remote(ExtCleanInfo.source_id) == foreign(ExtDatasource.source_id)",
        # uselist=False,
    )


class ExtTargetInfo(CRUDMixin, db.Model):
    """
    目标表的基础信息表
    """

    target_table = Column(VARCHAR(50))
    remark = Column(VARCHAR(1000))
    weight = Column(Integer)
    sync_column = Column(String(200))
    date_column = Column(String(50))


class ExtHistoryTask(CRUDMixin, db.Model):
    """
    记录抓历史数据的任务
    task_type: 1 抓数和入库  2 抓数 3 入库
    status: 1 完成 2 取消 3 开始
    """

    source_id = Column(String(15))
    task_id = Column(String(50))
    task_type = Column(Integer)
    target_table = Column(String(500))
    ext_start = Column(String(50))
    ext_end = Column(String(50))
    task_start = Column(DateTime)
    task_end = Column(DateTime)
    status = Column(Integer)
    remark = Column(String(1000))


class ExtHistoryLog(CRUDMixin, db.Model):
    """
    记录抓取历史数据的每一天日志
    result = 1 成功 2 失败
    """

    source_id = Column(String(15))
    task_id = Column(String(50))
    ext_date = Column(String(50))
    result = Column(Integer)
    success_table = Column(String(500))
    fail_table = Column(String(500))
    remark = Column(String(2000))


class ExtTestQuery(CRUDMixin, db.Model):
    source_id = Column(String(15))
    target_table = Column(VARCHAR(50))
    query_sql = Column(VARCHAR(1000))


class ExtCheckNum(CRUDMixin, db.Model):
    source_id = Column(String(15))
    date = Column(DateTime)
    num = Column(Integer)


class ExtParmPlatform(CRUDMixin, db.Model):
    cmid = Column(Integer)
    foreign_store_id = Column(String(50))
    foreign_item_id = Column(String(512))
    item_name = Column(String(512))
    mini_show = Column(Integer, default=0)
    safety_stock_count = Column(Integer)
    promotions = Column(Boolean)
    seasonal = Column(String(24))
    is_valid = Column(Boolean)
    specification = Column(Integer)
    safety_stock_days = Column(Integer)
    delivery = Column(String(24))


class DeliveryPeriod(CRUDMixin, db.Model):
    cmid = Column(Integer)
    foreign_store_id = Column(String(50))
    delivery_period = Column(String(64), default='')
