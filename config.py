from marshmallow import Schema, fields, post_load


class BaseConfig(Schema):
    def __getattr__(self, item):
        return self.data.get(item)
    project_id = fields.Str(required = True)
    dataset_id = fields.Str(required = True)
    table_id = fields.Str(required = True)
    page_count = fields.Int(required = False)
    api_key_filename = fields.Str(required = True)
    api_key_bucket_name = fields.Str(required = True)
    url_base = fields.Str(required = True)
    page_limit = fields.Int(required = False)

    @post_load
    def make_config(self, data, **kwargs):
        self.data = data
        return self

def load_config(config_path= "config.json") -> BaseConfig:
    with open( config_path, 'r' ) as file:
        config_str = file.read()
        config = BaseConfig().loads(config_str)

        return config