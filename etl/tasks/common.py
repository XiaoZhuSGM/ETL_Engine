import json
import boto3
from botocore.client import Config

lambda_client = boto3.client(
    "lambda", config=Config(connect_timeout=930, read_timeout=930, retries=dict(max_attempts=0))
)


def lambda_invoker(function_name, payload, type="RequestResponse", qualifier="prod"):
    response = lambda_client.invoke(
        FunctionName=function_name,
        InvocationType=type,
        Payload=json.dumps(payload),
        Qualifier=qualifier,
    )
    response["Payload"] = json.load(response["Payload"])
    return response
