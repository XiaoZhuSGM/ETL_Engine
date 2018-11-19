from etl.models import session_scope
from ..etl import db
from etl.models.etl_table import ExtParamPlatform


class DisplayInfoExist(Exception):
    def __str__(self):
        return "该数据已经存在"


class DisplayInfo:
    @staticmethod
    def get_cmid():
        ext_display_info = ExtParamPlatform.query.all()
        cmid = list(set([item.cmid for item in ext_display_info]))
        cmid.sort()
        return list(cmid)

    @staticmethod
    def get_store_id_from_cmid(cmid):
        ext_display_info = ExtParamPlatform.query.filter_by(cmid=cmid).order_by(ExtParamPlatform.id.asc()).all()
        if not ext_display_info:
            return None
        foreign_store_id = list(set([item.foreign_store_id for item in ext_display_info]))
        foreign_store_id.sort()
        return foreign_store_id

    @staticmethod
    def get_info_from_store_id(cmid, foreign_store_id):
        ext_display_info = ExtParamPlatform.query.filter_by(cmid=cmid, foreign_store_id=foreign_store_id).all()
        if not ext_display_info:
            return None

        return [item.to_dict() for item in ext_display_info]

    @session_scope
    def create(self, **info):
        cmid = info.get('cmid')
        foreign_store_id = info.get('foreign_store_id')
        foreign_item_id = info.get('foreign_item_id')
        ext_display_info = ExtParamPlatform.query.filter_by(cmid=cmid,
                                                            foreign_store_id=foreign_store_id,
                                                            foreign_item_id=foreign_item_id).first()
        if ext_display_info:
            raise DisplayInfoExist

        ExtParamPlatform(**info).save()

    @session_scope
    def delete_info(self, id_list):
        ExtParamPlatform.query.filter(ExtParamPlatform.id.in_(id_list)).delete(synchronize_session=False)

    @session_scope
    def update_info(self, **info):
        id = info.get("id")
        params = info.get("params")
        ExtParamPlatform.query.filter_by(id=id).update(params)

    def find_by_page_limit(self, page, per_page, cmid, foreign_store_id):
        if page == -1 and per_page == -1:
            data_list = self.find_all(cmid, foreign_store_id)
            store_data = [data.to_dict() for data in data_list]
            return store_data

        pagination = ExtParamPlatform.query.filter_by(cmid=cmid, foreign_store_id=foreign_store_id). \
            order_by(ExtParamPlatform.id.asc()
                     ).paginate(page, per_page=per_page, error_out=False)
        params = pagination.items
        total_page = pagination.pages
        return dict(
            items=[param.to_dict() for param in params],
            cur_page=page,
            total_page=total_page
        )

    def find_all(self, cmid, foreign_store_id):
        data_list = (
            ExtParamPlatform.query.filter_by(cmid=cmid, foreign_store_id=foreign_store_id)
                .order_by(ExtParamPlatform.id.asc())
                .all()
        )
        return data_list
