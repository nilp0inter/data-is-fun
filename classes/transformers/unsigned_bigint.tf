[transformer]
name=MySQL unsigned bigint
compatible_writers=mysql
regexp=^(?P<data>\d+)$
formatter=%(data)s
output_type=unsigned_bigint
type_format=BIGINT UNSIGNED

[functions]
matcher_data=lambda x: 0 <= int(x['data']) <= 18446744073709551615
size=lambda x: 8
