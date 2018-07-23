from etl.models.ext_datasource_con import ExtDatasourceCon
from etl.models import session_scope


class ExtDatasourceConNotExist(Exception):
    def __str__(self):
        return "ext_datasource_con not found"


class ExtDatasourceConService:
    def default_dictify(self, ext_datasource_con):
        return {
            "id": ext_datasource_con.id,
            "source_id": ext_datasource_con.source_id,
            "cmid": ext_datasource_con.cmid,
            "roll_back": ext_datasource_con.roll_back,
            "frequency": ext_datasource_con.frequency,
            "period": ext_datasource_con.period,
        }

    @session_scope
    def create_ext_datasource_con(self, info):
        """创建 ext_datasource_con.

        :param info: info 值.
        :type info: dict
        :return: ExtDatasourceCon
        :rtype: ExtDatasourceCon
        """

        ext_datasource_con = ExtDatasourceCon(**info)
        ext_datasource_con.save()
        return ext_datasource_con

    def get_ext_datasource_con(self, cmid):
        """获取单个 ext_datasource_con.

        :param id: ExtDatasourceCon.cmid
        :type id: int
        :raises ExtDatasourceConNotExist: ExtDatasourceCon 不存在
        :return: ExtDatasourceCon 的详情
        :rtype: dict
        """

        ext_datasource_con = ExtDatasourceCon.query.filter_by(cmid=cmid).first()
        if not ext_datasource_con:
            raise ExtDatasourceConNotExist()
        return self.default_dictify(ext_datasource_con)

    @session_scope
    def modify_ext_datasource_con(self, id, info):
        """修改单个 ext_datasource_con.

        :param id: ExtDatasourceCon.id
        :type id: int
        :param info: 要修改的内容
        :type info: dict
        :raises ExtDatasourceConNotExist: ExtDatasourceCon.id 不存在
        :return: ExtDatasourceCon
        :rtype: ExtDatasourceCon
        """

        ext_datasource_con = ExtDatasourceCon.query.get(id)
        if not ext_datasource_con:
            raise ExtDatasourceConNotExist()
        ext_datasource_con.update(**info)
        return ext_datasource_con