[transformer]
name=MySQL signed int
regexp=^(?P<data>-?\d+)$
formatter=%(data)s
output_format=INT

[functions]
matcher_data=lambda x: -2147483648 <= int(x['data']) <= 2147483647
size=lambda x: 4
