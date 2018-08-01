
from flask_inputs.validators import JsonSchema
from flask_inputs import Inputs

PROPERTIES = {
    "source_id": {"type": "string"},
    "roll_back": {"type": "integer"},
    "frequency": {"type": "integer"},
    "period": {"type": "integer"},
}


class GetExtDatasourceCon(Inputs):
    _schema = {
        "type": "object",
        "properties": {"cmid": {"type": "string"}},
        "required": ["cmid"],
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
