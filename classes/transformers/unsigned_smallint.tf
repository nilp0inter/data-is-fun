[transformer]
name=MySQL unsigned smallint
regexp=^(?P<data>\d+)$
formatter=%(data)s
output_format=SMALLINT UNSIGNED

[functions]
matcher_data=lambda x: 0 <= int(x['data']) <= 65535
size=lambda x: 2
