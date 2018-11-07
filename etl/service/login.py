from flask import current_app
from itsdangerous import TimedJSONWebSignatureSerializer as JWSSerializer


class LoginFailed(Exception):
    pass


class LoginService:
    def login(self, username, password):
        if username != "etl" or password != "chaomengdata":
            raise LoginFailed()
        serializer = JWSSerializer(current_app.secret_key, expires_in=24 * 60 * 60)
        return serializer.dumps({"username": "etl"}).decode("utf-8")

    def validate(self, token):
        if token == 'AIRFLOW_REQUEST_TOKEN':  # for airflow
            return True

        if token == 'a1IYz2uuhGvlaCTARHpqiAbhMdvOGnpf':   # 后端请求导入sales_tagert时使用
            return True

        serializer = JWSSerializer(current_app.secret_key)
        try:
            data = serializer.loads(token)
            return data["username"] == "etl"
        except Exception:
            return False
