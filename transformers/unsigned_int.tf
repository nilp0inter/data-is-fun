[transformer]
name=MySQL unsigned int
compatible_writers=mysql
regexp=^(?P<data>\d+)$
formatter=%(data)s
output_type=unsigned_int
type_format=INT UNSIGNED

[functions]
matcher_data=lambda x: 0 <= int(x['data']) <= 4294967295
size=lambda x: 4
