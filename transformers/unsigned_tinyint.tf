[transformer]
name=MySQL unsigned tinyint
compatible_writers=mysql
regexp=^(?P<data>\d+)$
formatter=%(data)s
output_type=unsigned_tinyint
type_format=TINYINT UNSIGNED

[functions]
matcher_data=lambda x: 0 <= int(x['data']) <= 255
size=lambda x: 1
