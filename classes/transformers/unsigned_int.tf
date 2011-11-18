[transformer]
name=MySQL unsigned int
regexp=^(?P<data>\d+)$
formatter=%(data)s
output_format=int unsigned

[functions]
matcher_data=lambda x: 0 <= int(x) <= 4294967295
size=lambda x: 4
