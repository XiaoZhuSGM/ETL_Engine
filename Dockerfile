
FROM python:3.6

LABEL Name=etl-engine Version=0.0.1

EXPOSE 5000

ENV PYMSSQL_BUILD_WITH_BUNDLED_FREETDS=1

RUN apt-get update \
    && apt-get install -yqq libaio1 unzip \
    && mkdir -p /opt/oracle \
    && cd /opt/oracle \
    && wget https://s3.cn-north-1.amazonaws.com.cn/ext-etl-data/instantclient-basic-linux.x64-12.1.0.2.0.zip \
    && unzip instantclient-basic-linux.x64-12.1.0.2.0.zip \
    && cd /opt/oracle/instantclient_12_1 \
    && ln -s libclntsh.so.12.1 libclntsh.so \
    && ln -s libocci.so.12.1 libocci.so \
    && echo /opt/oracle/instantclient_12_1 > /etc/ld.so.conf.d/oracle-instantclient.conf \
    && ldconfig

ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone


WORKDIR /app
ADD requirements.txt /app
RUN python3 -m pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
ADD . /app



CMD ["gunicorn", "-c", "gunicorn.py", "manage:app"]
# CMD ["uwsgi", "--http", "0.0.0.0:5000", "--wsgi-file", "manage.py", "--callable", "app", "--processes", "1", "--stats", "0.0.0.0:9191"]
