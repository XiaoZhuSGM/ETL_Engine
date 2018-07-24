# 超盟 抓数入库策略引擎

## Choose python version 3.6

## fabfile.py:

### 在本地使用，用于控制远程机器打包

## 启动
python manage.py runserver
### 交互使用
python manage.py shell  or python manage.py db

## 迁移数据库

### python manage.py db init 初始化数据库，会在数据库里生成版本号，版本号用来控制每一次的迁移
### python manage.py db migrate 数据库迁移，生成脚本
### python manage.py db upgrade 执行SQL到数据库
### python manage.py db --help 查看帮助
### 更多内容点击链接查看
[flask-migrate](http://flask-migrate.readthedocs.io/en/latest/)

## 部署

```bash
fab deploy --env dev --user <yourname>
```

## config包
### 配置参数，包括数据库配置，环境变量配置，其他参数配置

## etl包
### controllers

 1. api包为提供日志报警接口，供公司其他项目使用
 2. admin_api包为数据组策略引擎配置API相关接口


### service包
 业务逻辑

### dao包
    数据库操作

### validators
数据效颜，检查前端传来的数据是否正确，在API层通过装饰器使用

### models
数据库ORM对象关系定义

## test包
单元测试
