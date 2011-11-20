[transformer]
name=MySQL unsigned tinyint
regexp=^(?P<data>\d+)$
formatter=%(data)s
output_format=TINYINT UNSIGNED

[functions]
matcher_data=lambda x: 0 <= int(x['data']) <= 255
size=lambda x: 1
