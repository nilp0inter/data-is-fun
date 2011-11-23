[transformer]
name=MySQL signed mediumint
compatible_writers=mysql
regexp=^(?P<data>-?\d+)$
formatter=%(data)s
output_type=mediumint
type_format=MEDIUMINT

[functions]
matcher_data=lambda x: -8388608 <= int(x['data']) <= 8388607
size=lambda x: 3
