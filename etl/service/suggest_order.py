import arrow
import boto3
import re

import tempfile
import pandas as pd
import numpy as np


class SuggestOrderNotExist(Exception):
    def __str__(self):
        return "建议订单不存在"


class SuggestOrderItemExist(Exception):
    def __str__(self):
        return 'foreign_item_id 已存在'


class SuggestOrderService(object):
    s3 = boto3.resource("s3")
    bucket = 'suggest-order'
    date = arrow.now().format('YYYY-MM-DD')
    key = "dist/{cmid}/{store_id}/{date}.csv.gz"

    @staticmethod
    def data_valid(data):
        foreign_item_id = data.get('foreign_item_id')
        fill_qty = data.get('fill_qty')
        show_code = data.get('show_code')
        cmid = data.get('cmid')
        store_id = data.get('store_id')

        if None in [foreign_item_id, fill_qty, show_code, cmid, store_id]:
            raise SuggestOrderNotExist

    def show_cmid(self):
        cmid_ = set()
        objects = self.s3.Bucket(self.bucket).objects.filter(Prefix="dist")
        for obj in objects:
            cmid = re.findall("dist/(\d+)/", obj.key)
            if cmid:
                cmid_.add(cmid[0])

        return list(cmid_)

    def show_store_id(self, cmid):
        store_id_ = set()
        objects = self.s3.Bucket(self.bucket).objects.filter(Prefix=f"dist/{cmid}")
        for obj in objects:
            store_id = re.findall("dist/\d+/(\d+)/", obj.key)
            if store_id:
                store_id_.add(store_id[0])

        return list(store_id_)

    def show_info_dataframe(self, cmid, store_id):
        objects = self.s3.Bucket(self.bucket).objects.filter(Prefix=f'dist/{cmid}/{store_id}')
        for obj in sorted(
                objects, key=lambda obj: int(obj.last_modified.strftime("%s")), reverse=True
        ):
            if self.date in obj.key:
                key = obj.key
                break
            else:
                raise SuggestOrderNotExist
        s3_url = f"s3://{self.bucket}/{key}"
        dataframe = pd.read_csv(s3_url,
                                compression="gzip",
                                dtype={'foreign_item_id': str, 'fill_qty': str},
                                usecols=["foreign_item_id", "fill_qty", "show_code"])
        return dataframe

    def show_info_list(self, cmid, store_id):
        dataframe = self.show_info_dataframe(cmid, store_id)
        data_list = np.array(dataframe)
        data = data_list.tolist()

        return data

    def suggest_order_add(self, **data):
        cmid = data.get('cmid')
        store_id = data.get('store_id')
        foreign_item_id = data.get('foreign_item_id')

        data_list = self.show_info_list(cmid, store_id)

        foreign_item_id_list = [item[0] for item in data_list]
        if foreign_item_id in foreign_item_id_list:
            raise SuggestOrderItemExist

        del data['cmid']
        del data['store_id']

        dataframe = self.show_info_dataframe(cmid, store_id)
        dataframe = dataframe.append(data, ignore_index=True)

        return dataframe

    def suggest_order_remove(self, **data):
        cmid = data.get('cmid')
        store_id = data.get('store_id')
        foreign_item_id = data.get('foreign_item_id')

        data_list = self.show_info_list(cmid, store_id)
        data = [item for item in data_list
                if foreign_item_id not in item]
        dataframe = pd.DataFrame(data, columns=['foreign_item_id', 'fill_qty', 'show_code'])

        return dataframe

    def suggest_order_update(self, **data):
        dataframe = self.suggest_order_remove(**data)

        del data['cmid']
        del data['store_id']

        dataframe = dataframe.append(data, ignore_index=True)

        return dataframe

    def upload_to_s3(self, dataframe, **data):
        cmid = data.get('cmid')
        store_id = data.get('store_id')
        filename = tempfile.NamedTemporaryFile(mode="w", encoding="utf-8")
        dataframe.to_csv(filename, compression="gzip", index=False)
        key = self.key.format(cmid=cmid, store_id=store_id, date=self.date)
        self.s3.Bucket(self.bucket).upload_file(filename.name, key)
