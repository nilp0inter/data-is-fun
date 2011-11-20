[transformer]
name=MySQL signed mediumint
regexp=^(?P<data>-?\d+)$
formatter=%(data)s
output_format=MEDIUMINT

[functions]
matcher_data=lambda x: -8388608 <= int(x['data']) <= 8388607
size=lambda x: 3
