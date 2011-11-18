[transformer]
name=MySQL signed smallint
regexp=^(?P<data>-?\d+)$
formatter=%(data)s
output_format=smallint signed

[functions]
matcher_data=lambda x: -32768 <= int(x) <= 32767
