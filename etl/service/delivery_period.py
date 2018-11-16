from etl.models import session_scope
from etl.models.etl_table import DeliveryPeriod


class DeliveryPeriodExist(Exception):
    def __str__(self):
        return 'foreign_store_id 已经存在'


class DeliveryPeriodService(object):
    @staticmethod
    def get_cmid():
        delivery_period = DeliveryPeriod.query.all()
        cmid = list(set([item.cmid for item in delivery_period]))
        cmid.sort()
        return list(cmid)

    @staticmethod
    def get_store_id(cmid):
        delivery_period = DeliveryPeriod.query.filter_by(cmid=cmid).all()
        if not delivery_period:
            return None

        return [item.to_dict() for item in delivery_period]

    @session_scope
    def create(self, **info):
        cmid = info.get('cmid')
        foreign_store_id = info.get('foreign_store_id')
        ext_display_info = DeliveryPeriod.query.filter_by(cmid=cmid,
                                                          foreign_store_id=foreign_store_id, ).first()
        if ext_display_info:
            raise DeliveryPeriodExist

        DeliveryPeriod(**info).save()

    @session_scope
    def delete(self, id_list):
        DeliveryPeriod.query.filter(DeliveryPeriod.id.in_(id_list)).delete(synchronize_session=False)

    @session_scope
    def update_info(self, **info):
        id = info.get("id")
        params = info.get("params")
        DeliveryPeriod.query.filter_by(id=id).update(params)
