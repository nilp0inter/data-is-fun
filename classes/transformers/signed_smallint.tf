[transformer]
name=MySQL signed smallint
compatible_writers=mysql
regexp=^(?P<data>-?\d+)$
formatter=%(data)s
output_type=smallint
type_format=SMALLINT

[functions]
matcher_data=lambda x: -32768 <= int(x['data']) <= 32767
size=lambda x: 2
