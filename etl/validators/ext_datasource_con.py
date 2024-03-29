from flask_inputs import Inputs
from flask_inputs.validators import JsonSchema

PROPERTIES = {
    "source_id": {"type": "string"},
    "roll_back": {"type": "integer"},
    "frequency": {"type": "integer"},
    "period": {"type": "integer"},
}


class GetExtDatasourceCon(Inputs):
    _schema = {
        "type": "object",
        "properties": {"source_id": {"type": "string"}},
        "required": ["source_id"],
    }
    args = [JsonSchema(schema=_schema)]


class CreateExtDatasourceCon(Inputs):
    _schema = {
        "type": "object",
        "required": list(PROPERTIES.keys()),
        "properties": PROPERTIES,
    }

    json = [JsonSchema(schema=_schema)]


class ModifyExtDatasourceCon(Inputs):
    _schema = {"type": "object", "properties": PROPERTIES}
    json = [JsonSchema(schema=_schema)]
