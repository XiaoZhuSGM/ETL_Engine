from etl.models import session_scope
from etl.models.etl_table import DeliveryPeriod


class DeliveryPeriodExist(Exception):
    def __str__(self):
        return 'foreign_store_id 已经存在'


class DeliveryPeriodNotExist(Exception):
    def __str__(self):
        return 'foreign_store_id 不存在'


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

    @staticmethod
    def get_info_from_store_id(cmid, foreign_store_id):
        delivery_period = DeliveryPeriod.query.filter_by(cmid=cmid,
                                                         foreign_store_id=foreign_store_id).first()
        if not delivery_period:
            raise DeliveryPeriodNotExist

        return delivery_period.to_dict()

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

    def find_by_page_limit(self, page, per_page, cmid):
        if page == -1 and per_page == -1:
            data_list = self.find_all(cmid)
            return dict(
                items=[data.to_dict() for data in data_list],
                cur_page='',
                total_page=1,
            )

        pagination = DeliveryPeriod.query.filter_by(cmid=cmid).order_by(
            DeliveryPeriod.id.asc()).paginate(page, per_page=per_page, error_out=False)
        params = pagination.items
        total_page = pagination.pages
        return dict(
            items=[param.to_dict() for param in params],
            cur_page=page,
            total_page=total_page
        )

    def find_all(self, cmid):
        data_list = (
            DeliveryPeriod.query.filter_by(cmid=cmid)
                .order_by(DeliveryPeriod.id.asc())
                .all()
        )
        return data_list
