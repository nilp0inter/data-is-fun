[transformer]
name=MySQL signed int
compatible_writers=mysql
regexp=^(?P<data>-?\d+)$
formatter=%(data)s
output_type=int
type_format=INT

[functions]
matcher_data=lambda x: -2147483648 <= int(x['data']) <= 2147483647
size=lambda x: 4
