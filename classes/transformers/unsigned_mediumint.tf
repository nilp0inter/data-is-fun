[transformer]
name=MySQL unsigned mediumint
regexp=^(?P<data>\d+)$
formatter=%(data)s
output_format=mediumint unsigned

[functions]
matcher_data=lambda x: 0 <= int(x) <= 16777215
