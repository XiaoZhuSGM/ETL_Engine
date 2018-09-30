
FROM python:3.6

LABEL Name=etl-engine Version=0.0.1

EXPOSE 5000

WORKDIR /app
ADD . /app

ENV PYMSSQL_BUILD_WITH_BUNDLED_FREETDS=1
RUN python3 -m pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
***REMOVED***
***REMOVED***
ENV AWS_DEFAULT_REGION=cn-north-1

RUN apt-get update \
    && apt-get install -yqq libaio1 unzip \
    && mkdir -p /opt/oracle \
    && cd /opt/oracle \
    && wget https://s3.cn-north-1.amazonaws.com.cn/ext-etl-data/instantclient-basic-linux.x64-12.1.0.2.0.zip \
    && unzip instantclient-basic-linux.x64-12.1.0.2.0.zip \
    && echo /opt/oracle/instantclient_12_2 > /etc/ld.so.conf.d/oracle-instantclient.conf \
    && ldconfig


CMD ["gunicorn", "-c", "gunicorn.py", "manage:app"]
# CMD ["uwsgi", "--http", "0.0.0.0:5000", "--wsgi-file", "manage.py", "--callable", "app", "--processes", "1", "--stats", "0.0.0.0:9191"]
