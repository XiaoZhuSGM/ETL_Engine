from sqlalchemy import VARCHAR, REAL, Integer, DateTime, String, Column
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from etl.etl import db
from .base import CRUDMixin


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
    result = Column(Integer)
    remark = Column(VARCHAR(1000))


class ExtCleanInfo(CRUDMixin, db.Model):
    """
    合成目标表所需要信息
    like   {"t_sl_master": ['fbrh_no', 'fflow_no', 'ftrade_date', 'fcr_time', 'fsell_way'],
                "t_sl_detail": ['fprice', 'fpack_qty', 'famt', 'fflow_no', 'fitem_subno', 'fitem_id'],
                "t_br_master": ['fbrh_name', 'fbrh_no'],
                "t_bi_master": ['fitem_id', 'fitem_subno', 'fitem_name', 'funit_no', 'fitem_clsno'],
                "t_bc_master": ['fitem_clsno', 'fitem_clsname', 'fprt_no'],
                "t_bi_barcode": ['funit_qty', 'fitem_id', 'fitem_subno']}
    like     {"t_sl_master": {"fbrh_no": str}, "t_br_master": {"fbrh_no": str},
           "t_bi_master": {"fitem_clsno": str},
           "t_bc_master": {"fitem_clsno": str, "fprt_no": str}}
    """
    source_id = Column(String(15))
    origin_table = Column(JSONB, comment="合成目标表需要的原始表何所需要的字段")
    covert_str = Column(JSONB, comment="需要格式转换的字段，防止pandas家在丢失数据位")
    target_table = Column(VARCHAR(50), comment="目标表，譬如goodsflow_32yyyyyyyyyyyyy,chain_goods等")
