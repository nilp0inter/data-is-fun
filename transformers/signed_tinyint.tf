[transformer]
name=MySQL signed tinyint
compatible_writers=mysql
regexp=^(?P<data>-?\d+)$
formatter=%(data)s
output_type=tinyint
type_format=TINYINT


[functions]
matcher_data=lambda x: -128 <= int(x['data']) <= 127
size=lambda x: 1
