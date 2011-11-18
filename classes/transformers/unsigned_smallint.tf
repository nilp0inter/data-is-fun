[transformer]
name=MySQL unsigned smallint
regexp=^(?P<data>\d+)$
formatter=%(data)s
output_format=smallint unsigned

[functions]
matcher_data=lambda x: 0 <= int(x) <= 65535
