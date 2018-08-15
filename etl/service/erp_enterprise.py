# -*- coding: utf-8 -*-
# @Time    : 2018/8/14 下午2:19
# @Author  : 范佳楠
from ..etl import db
from ..models import session_scope
from ..models.etl_table import ExtErpEnterprise


class ErpEnterpriseNotExist(Exception):
    def __str__(self):
        return "erp_enterprise not found"


class ErpEnterpriseService(object):

    @session_scope
    def add_enterprise(self, enterprise_json):
        enterprise = ExtErpEnterprise(**enterprise_json)
        enterprise.save()
        return True

    def get_enterprise_by_id(self, enterprise_id):
        enterprise = ExtErpEnterprise.query.filter_by(id=enterprise_id).one_or_none()

        if not enterprise:
            raise ErpEnterpriseNotExist()

        return enterprise.to_dict()

    @session_scope
    def update_enterprise(self, enterprise_id, new_enterprise_json):
        enterprise = ExtErpEnterprise.query.filter_by(id=enterprise_id).one_or_none()

        if not enterprise:
            raise ErpEnterpriseNotExist()

        enterprise.update(**new_enterprise_json)

    def find_all(self):
        enterprise_list = db.session.query(ExtErpEnterprise).order_by(ExtErpEnterprise.id.asc()).all()
        return enterprise_list

    def find_by_page_limit(self, page, per_page):
        pagination = ExtErpEnterprise.query.order_by(ExtErpEnterprise.id.asc()).paginate(page,
                                                                                         per_page=per_page,
                                                                                         error_out=False)
        enterprise_list = pagination.items
        total = pagination.total
        return dict(items=[enterprise.to_dict() for enterprise in enterprise_list], total=total)
