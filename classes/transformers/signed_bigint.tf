[transformer]
name=MySQL signed bigint
compatible_writers=mysql
regexp=^(?P<data>-?\d+)$
formatter=%(data)s
output_type=bigint
type_format=BIGINT

[functions]
matcher_data=lambda x: -9223372036854775808 <= int(x['data']) <= 9223372036854775807
size=lambda x: 8
