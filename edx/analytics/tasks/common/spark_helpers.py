from pyspark.sql.types import ArrayType, StructType


def flatten_schema(schema, prefix=None):
    fields = []
    for field in schema.fields:
        name = prefix + '.' + field.name if prefix else field.name
        dtype = field.dataType
        if isinstance(dtype, ArrayType):
            dtype = dtype.elementType
        if isinstance(dtype, StructType):
            fields += flatten_schema(dtype, prefix=name)
        else:
            fields.append(name)
    return fields
