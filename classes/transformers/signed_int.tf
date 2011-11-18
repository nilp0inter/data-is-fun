[transformer]
name=MySQL signed int
regexp=^(?P<data>-?\d+)$
formatter=%(data)s
output_format=int signed

[functions]
matcher_data=lambda x: -2147483648 <= int(x) <= 2147483647
