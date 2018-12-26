import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from typing import Dict
from config.config import config

REDSHIFT_URL = config['prod']['SQLALCHEMY_DATABASE_URI']
GOODSFLOW_SQL_TEMPLATE = """
select to_char(saletime, 'yyyy-MM-dd'), count(*)
from goodsflow_{source_id}
where to_char(saletime, 'yyyy-MM-dd') >= '{start_date}'
  and to_char(saletime, 'yyyy-MM-dd') < '{end_date}'
group by to_char(saletime, 'yyyy-MM-dd') order by to_char(saletime, 'yyyy-MM-dd')
"""
COST_SQL_TEMPLATE = """
select date, count(*)
from cost_{source_id}
where date >= '{start_date}'
  and date < '{end_date}'
group by date
order by date;
"""


class IQRService(object):
    def __init__(self, source_id):
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        self.goodflow_sql = GOODSFLOW_SQL_TEMPLATE.format(
            source_id=source_id, start_date=start_date, end_date=end_date
        )
        self.cost_sql = COST_SQL_TEMPLATE.format(
            source_id=source_id, start_date=start_date, end_date=end_date
        )
        self.engine = create_engine(REDSHIFT_URL)

    def load_frame(self, sql: str) -> pd.DataFrame:
        return pd.read_sql(sql, self.engine)

    def std(self, frame: pd.DataFrame) -> float:
        """
        均值方差计算
        """
        return frame["count"].mean() - 2 * frame["count"].std()

    def quadrature(self, frame) -> float:
        """
        四分位数计算
        """
        quantiles = frame["count"].quantile([0.25, 0.5, 0.75])
        Q1 = quantiles[0.25]
        Q3 = quantiles[0.75]
        IQR = Q3 - Q1
        return Q1 - 1.5 * IQR

    def median(self, frame):
        return frame["count"].median()

    def pipeline(self) -> dict:
        result: Dict[str, dict] = {"goodsflow": dict(), "cost": dict()}
        goodsflow_frame = self.load_frame(self.goodflow_sql)
        cost_frame = self.load_frame(self.cost_sql)

        result["goodsflow"]["std"] = self.std(goodsflow_frame)
        result["goodsflow"]["quantile"] = self.quadrature(goodsflow_frame)
        result["goodsflow"]["median"] = self.median(goodsflow_frame)

        result["cost"]["std"] = self.std(cost_frame)
        result["cost"]["quantile"] = self.quadrature(cost_frame)
        result["cost"]["median"] = self.median(goodsflow_frame)

        return result

