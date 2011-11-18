[transformer]
name=MySQL unsigned bigint
regexp=^(?P<data>\d+)$
formatter=%(data)s
output_format=bigint unsigned

[functions]
matcher_data=lambda x: 0 <= int(x) <= 18446744073709551615
