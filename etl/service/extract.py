# -*- coding: utf-8 -*-
# @Time    : 2018/8/8 上午10:58
# @Author  : 范佳楠

import json
from datetime import datetime, timedelta

import boto3
from sqlalchemy import create_engine

from ..models.ext_table_info import ExtTableInfo
from ..models.datasource import ExtDatasource
from ..dao.datasource import DatasourceDao

datasource_dao = DatasourceDao()

BUCKET_NAME = 'fanjianan'
FULL_SQL = 'data/{source_id}/date={time}/full.json'
boto_client = boto3.client('s3')
SQL_SERVER_TEMPLATE = """
    select * from
    (
        select *,row_number() over
        (
            order by
            {order_columns} desc
        ) rowno
        from {table_name}
    ) hhh
    where hhh.rowno >= {start_row}
    and hhh.rowno <= {end_row}
"""

ORACLE_TEMPLATE = """
    SELECT *
  FROM (SELECT ROWNUM AS rowno, t.*
          FROM ALCDIFFLOG t
         WHERE ROWNUM <= {end_row}) table_alias
 WHERE table_alias.rowno >= {start_row}
"""


def get_now_yesterday_format(filter_format):
    now = datetime.now()
    yesterday = now + timedelta(days=-1)
    if filter_format == 'yyyy-MM-dd':
        record_time = yesterday.strftime('%Y-%m-%d')
        date_s = record_time
        date_e = now.strftime('%Y-%m-%d')
        return record_time, date_s, date_e
    elif filter_format == 'yyyyMMdd':
        record_time = yesterday.strftime('%Y%m%d')
        date_s = record_time
        date_e = now.strftime('%Y%m%d')
        return record_time, date_s, date_e
    else:
        pass


def generator_sql_service(source_id):
    # 获取/fanjianan/data/source_id/yyyy-MM-dd/full.json is exist
    now = datetime.now()
    yesterday = now + timedelta(days=-1)
    extract_time = yesterday.strftime('%Y-%m-%d')
    key = FULL_SQL.format(source_id=source_id, time=extract_time)
    try:
        # 说明我们进行过今天的全量了，那么我们需要生成同步sql
        response = boto_client.get_object(Bucket=BUCKET_NAME, Key=key)

    except Exception as e:
        print('generator_sql', e)
        return generator_full_sql(source_id)


def generator_full_sql(source_id):
    now = datetime.now()
    yesterday = now + timedelta(days=-1)
    extract_time = yesterday.strftime('%Y-%m-%d')

    table_info_list = ExtTableInfo.query.filter_by(source_id=source_id).all()
    datasource = datasource_dao.find_datasource_by_source_id(source_id)
    database_url = get_datasource_url(datasource)
    engine = create_engine(database_url)
    db_type = datasource.db_type
    # 一个datasource就是一个dict
    sql_dict = {'type': 'full', 'date': now.strftime('%Y-%m-%d')}

    for table_info in table_info_list:
        # 注意一张表可能会有多个sql语句,因为会有分页，所以一个表就是一个list,里面存放该表的sql语句
        # 首先判断该表是有效状态还是无效状态

        if table_info.weight is None or table_info.weight == 0 or table_info.weight == 2:
            continue

        current_table_sql_list = []
        # 判断当前对象是使用什么策略，通过策略可以判断对象是什么类型的表(日期/其他)
        table_name = table_info.table_name
        sql = "select * from {table_name}".format(table_name=table_name)
        order_columns = table_info.order_column

        '''
            strategy 1 全量抓取
                     2 单主键
                     3 复合主键
                     4 无主键
        '''

        if table_info.strategy == 1:
            current_table_sql_list.append(sql)
        else:
            '''
                如果不是全量抓取，说明这些表都是与日期相关的表，我们需要将日期进行渲染
            '''
            condition = table_info.filter
            filter_format = table_info.filter_format

            if condition:
                record_time, date_s, date_e = get_now_yesterday_format(filter_format)
                condition = condition.format(date_s=date_s, recorddate=record_time, date_e=date_e)
                sql += ' ' + condition

            # 这里我们需要考虑下数量的多少, 看是否需要进行分页,首先需要嗅探当天的数量有多少

            with engine.connect() as connection:
                # 创建出统计sql
                count_sql = sql.replace('*', 'count(*)')
                result = connection.execute(count_sql).fetchone()
                total = result[0]
                # 得到当前的总数之后, 我们需要进行判断是否需要分页
                if total > 200000:
                    # 这种情况需要分页, 我们设置好，如果总数超过了200000w条,那我们进行分页，每页10w条
                    page_count = total // 100000 if total % 100000 == 0 else total // 100000 + 1
                    for i in range(page_count):
                        start_row = i * 100000 + 1
                        end_row = (i + 1) * 100000
                        page_sql = get_paging_sql(db_type,
                                                  table_name,
                                                  start_row,
                                                  end_row,
                                                  condition,
                                                  order_columns)
                        current_table_sql_list.append(page_sql)
                else:
                    current_table_sql_list.append(sql)

        # 将当前表的所有sql语句加入到整个记录中
        sql_dict[table_name] = current_table_sql_list

    key = FULL_SQL.format(source_id=source_id, time=extract_time)
    boto_client.put_object(Body=json.dumps(sql_dict).encode(), Bucket=BUCKET_NAME, Key=key)
    return sql_dict


def get_paging_sql(db_type, table_name, start_row, end_row, condition, order_columns=None):
    page_condition = ''
    if condition:
        page_condition = condition.strip()
        page_condition = page_condition.replace('where', 'and (') + ')'

    if db_type == 'sqlserver':
        page_sql = SQL_SERVER_TEMPLATE.format(table_name=table_name,
                                              order_columns=order_columns,
                                              start_row=start_row,
                                              end_row=end_row)

        page_sql += ' ' + page_condition
        return page_sql
    elif db_type == 'oracle':
        page_sql = ORACLE_TEMPLATE.format(table_name=table_name,
                                          start_row=start_row,
                                          end_row=end_row)
        page_sql += ' ' + page_condition
        return page_sql
    else:
        pass


def get_datasource_url(datasource):
    db_type = datasource.db_type
    '''
        这里需要注意的是，目前我们没有出现多数据库的情况，所以我们这里会先写死，日后如果有，我们在考虑怎么来变化
    '''
    db_name = datasource.db_name[0]['database']
    username = datasource.username
    password = datasource.password
    host = datasource.host
    port = datasource.port

    if db_type == 'sqlserver':
        db_type = 'mssql+pymssql'

    elif db_type == 'postgresql':
        db_type += r'+psycopg2'
    elif db_type == 'oracle':
        db_type += r'+cx_oracle'
        db_name = r'?service_name=' + db_name

    database_url = "{db_type}://{username}:{password}@{host}:{port}/{db_name}".format(
        db_type=db_type,
        username=username,
        password=password,
        host=host,
        port=port,
        db_name=db_name)

    return database_url
