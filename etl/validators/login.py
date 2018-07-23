from flask_inputs import Inputs
from flask_inputs.validators import JsonSchema


class LoginInput(Inputs):
    _schema = {
        "type": "object",
        "properties": {"username": {"type": "string"}, "password": {"type": "string"}},
        "required": ["username", "password"],
    }
    json = [JsonSchema(schema=_schema)]
