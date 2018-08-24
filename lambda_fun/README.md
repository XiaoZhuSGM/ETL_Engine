# ETL lambda 发布脚本

## Choose python version 3.6

## fabfile.py:

### 在本地使用，用于控制lambda打包

## docker build

```bash
docker build -t tools:env .
```

## clean_data

清洗逻辑部分，主要用于清洗ERP的从源到目标

## extract_data

抓取远程ERP数据


## 发布
fab deploy_lambda:path=clean_data

## 远程创建lambda
fab create_lambda:lambda_name=clean_data

## 更新lambda
fab update_lambda:lambda_name=clean_data
