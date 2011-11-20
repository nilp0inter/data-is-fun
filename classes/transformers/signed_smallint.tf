[transformer]
name=MySQL signed smallint
regexp=^(?P<data>-?\d+)$
formatter=%(data)s
output_format=SMALLINT

[functions]
matcher_data=lambda x: -32768 <= int(x['data']) <= 32767
size=lambda x: 2
