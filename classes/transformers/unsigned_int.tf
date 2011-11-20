[transformer]
name=MySQL unsigned int
regexp=^(?P<data>\d+)$
formatter=%(data)s
output_format=INT UNSIGNED

[functions]
matcher_data=lambda x: 0 <= int(x['data']) <= 4294967295
size=lambda x: 4
