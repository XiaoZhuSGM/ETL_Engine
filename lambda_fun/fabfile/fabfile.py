from fabric.api import local
import os
import uuid

S3BUCKET = 'lambda-devenv'
LAMBDA_ROLE = 'arn:aws-cn:iam::244434823928:role/service-role/CM-DB-Dump'
LAMBDA_NAME = os.getcwd().rsplit('/', 1)[1]


def bundle():
    local('''
        rm -rf *.zip && \
        rm -rf bundle && \
        rm -f bundle.zip && \
        mkdir bundle && \
        cp -rf lambda_code/.venv/lib/python3.6/site-packages/* bundle && \
        rm -rf lambda_code/.venv
        zip -r9 -j bundle.zip lambda_code/* && \
        cd bundle && \
        zip -r9 ../bundle.zip * && \
        docker rm lambda_deploy
        ''')


def deploy_lambda(path="clean_data"):
    volumn = f"../{path}/*"
    local(f'''
        rm -rf ./lambda_code && mkdir ./lambda_code && \
        cp -r {volumn} ./lambda_code && \
        docker run --name lambda_deploy -v $PWD/lambda_code:/var/task -it tools:env  dinstall.sh && \
        fab bundle
    ''')


def create_lambda(timeout=30, memorysize=1024, lambda_name="clean_data"):
    unique_bundle_name = 'b-{}.zip'.format(str(uuid.uuid4()))
    local('cp bundle.zip {}'.format(unique_bundle_name))
    local(
        'aws s3 cp {bundle} s3://{bucket_name}/'
            .format(
            bundle=unique_bundle_name,
            bucket_name=S3BUCKET
        )
    )

    cmd = """
    aws lambda create-function \
        --function-name "{lambda_name}" \
        --runtime python3.6 \
        --handler {lambda_name}.handler \
        --role {role} \
        --code S3Bucket={bucket_name},S3Key={bundle} \
        --timeout {timeout} \
        --memory-size {memorysize}
    """.format(
        timeout=timeout,
        role=LAMBDA_ROLE,
        bucket_name=S3BUCKET,
        lambda_name=lambda_name,
        bundle=unique_bundle_name,
        memorysize=memorysize
    )
    local(cmd)
    _create_stage('dev')


def update_lambda(timeout=60, lambda_name="clean_data"):
    unique_bundle_name = 'b-{}.zip'.format(str(uuid.uuid4()))
    local('cp bundle.zip {}'.format(unique_bundle_name))
    local(
        'aws s3 cp {bundle} s3://{bucket_name}/'
            .format(
            bundle=unique_bundle_name,
            bucket_name=S3BUCKET
        )
    )
    cmd = """
    aws lambda update-function-code \
        --function-name "{lambda_name}" \
        --s3-bucket {bucket_name} \
        --s3-key {bundle}
    """.format(
        bucket_name=S3BUCKET,
        lambda_name=lambda_name,
        bundle=unique_bundle_name
    )
    local(cmd)


def _create_stage(stage='dev', lambda_name="clean_data"):
    cmd = """
    aws lambda create-alias \
            --function-name {lambda_name} \
            --name {stage} \
            --function-version '$LATEST'
    """.format(
        lambda_name=lambda_name,
        stage=stage
    )
    local(cmd)
