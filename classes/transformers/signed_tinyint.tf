[transformer]
name=MySQL signed tinyint
regexp=^(?P<data>-?\d+)$
formatter=%(data)s
output_format=tinyint signed

[functions]
matcher_data=lambda x: -128 <= int(x) <= 127
