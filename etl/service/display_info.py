import openpyxl
from etl.models import session_scope
from etl.models.etl_table import ExtParamPlatform
from common.common import ALLOWED_EXTENSIONS


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
            return dict(
                items=[data.to_dict() for data in data_list],
                cur_page='',
                total_page=1,
            )

        pagination = ExtParamPlatform.query.filter_by(cmid=cmid, foreign_store_id=foreign_store_id).order_by(
            ExtParamPlatform.id.asc()).paginate(page, per_page=per_page, error_out=False)
        params = pagination.items
        total_page = pagination.pages

        if page > total_page:
            page = total_page
            pagination = ExtParamPlatform.query.filter_by(cmid=cmid, foreign_store_id=foreign_store_id).order_by(
                ExtParamPlatform.id.asc()).paginate(page, per_page=per_page, error_out=False)
            params = pagination.items

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

    @session_scope
    def process_file(self, file):
        content = openpyxl.load_workbook(file)
        for row in content[content.sheetnames[0]].rows:
            item = dict()
            cmid = row[0].value
            if cmid == 'cmid':
                continue
            item['cmid'] = cmid
            item['foreign_store_id'] = str(row[1].value)
            item['foreign_item_id'] = str(row[2].value)
            item['item_name'] = row[3].value
            item['mini_show'] = row[4].value or 0
            item['safety_stock_count'] = row[5].value
            item['promotions'] = row[6].value
            item['seasonal'] = str(row[7].value)
            item['is_valid'] = row[8].value
            item['specification'] = row[9].value
            item['safety_stock_days'] = row[10].value
            item['delivery'] = str(row[11].value)

            print(item)

            result = ExtParamPlatform.query.filter_by(
                cmid=cmid,
                foreign_store_id=item['foreign_store_id'],
                foreign_item_id=item['foreign_item_id']
            ).update(item)

            if result == 0:
                ExtParamPlatform(**item).save()

    @staticmethod
    def allowed_file(filename):
        return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS
