from flask_inputs import Inputs
from flask_inputs.validators import JsonSchema


class GetEXtCleanInfos(Inputs):
    _schema = {
        "type": "object",
        "properties": {
            "source_id": {
                "type": "string"
            },
            "page": {
                "type": "string",
            },
            "per_page": {
                "type": "string",
            }
        },
        "required": ["source_id"]
    }


class CreateExtCleanInfo(Inputs):
    _schema = {
        "type": "object",
        "properties": {
            "source_id": {
                "type": "string"
            },
            "data": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "origin_table": {
                            "type": "string"
                        },
                        "columns": {
                            "type": "array",
                            "items": {
                                "type": "string"
                            },
                        },
                        "convert_str": {
                            "type": "object",
                            "patternProperties": {
                                ".*": {
                                    "type": "string"
                                },
                            },
                        },
                    },
                    "required": ["origin_table", "columns"],
                },

            },
        },
        "required": ["source_id", "data"]
    }
    json = [JsonSchema(schema=_schema)]


class ModiflyExtCleanInfo(Inputs):
    """
    [{"origin_table":"table1", "columns":["columns1", "columns2"], "convert_str":{"column1":"type1"}},
        {"origin_table":"table2", "columns":["columns1", "columns2"], "convert_str":{"column2":"type2"}},  ]
    """
    # _schema = {
    #     "type": "object",
    #     "properties": {
    #         "origin_table": {
    #             "type": "object",
    #             "patternProperties": {
    #                 ".*": {
    #                     "type": "array",
    #                     "items": {"type": "string"}
    #                 },
    #             },
    #         },
    #         "covert_str": {
    #             "type": "object",
    #             "patternProperties": {
    #                 ".*": {
    #                     "type": "object",
    #                     "patternProperties": {
    #                         ".*": {
    #                             "type": "string"
    #                         },
    #                     },
    #                 },
    #             },
    #         }
    #     },
    #     "required": ["origin_table"]
    # }
    _schema = {
        "type": "object",
        "properties": {
            "data": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "origin_table": {
                            "type": "string"
                        },
                        "columns": {
                            "type": "array",
                            "items": {
                                "type": "string"
                            },
                        },
                        "convert_str": {
                            "type": "object",
                            "patternProperties": {
                                ".*": {
                                    "type": "string"
                                },
                            },
                        },
                    },
                    "required": ["origin_table", "columns"],
                },

            },
        },
        "required": ["data"],
    }
    json = [JsonSchema(schema=_schema)]


class CopyExtCleanInfo(Inputs):
    _schema = {
        "type": "object",
        "properties": {
            "template_source_id": {"type": "string"},
            "target_source_id": {"type": "string"},
        },
        "required": ["template_source_id", "target_source_id"]
    }
    json = [JsonSchema(schema=_schema)]

