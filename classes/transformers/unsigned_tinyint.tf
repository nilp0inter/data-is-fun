[transformer]
name=MySQL unsigned tinyint
regexp=^(?P<data>\d+)$
formatter=%(data)s
output_format=tinyint unsigned

[functions]
matcher_data=lambda x: 0 <= int(x) <= 255
