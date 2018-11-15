from etl.models import session_scope
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
        ext_display_info = ExtParamPlatform.query.filter_by(cmid=cmid).all()
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
        ExtParamPlatform.query.filter_by(id=id).update(info)
