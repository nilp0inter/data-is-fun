[transformer]
name=MySQL unsigned bigint
regexp=^(?P<data>\d+)$
formatter=%(data)s
output_format=BIGINT UNSIGNED

[functions]
matcher_data=lambda x: 0 <= int(x['data']) <= 18446744073709551615
size=lambda x: 8
