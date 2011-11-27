[transformer]
name=MySQL unsigned mediumint
compatible_writers=mysql
regexp=^(?P<data>\d+)$
formatter=%(data)s
output_type=unsigned_mediumint
type_format=MEDIUMINT UNSIGNED

[functions]
matcher_data=lambda x: 0 <= int(x['data']) <= 16777215
size=lambda x: 3
