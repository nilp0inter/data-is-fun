[transformer]
name=MySQL signed mediumint
regexp=^(?P<data>-?\d+)$
formatter=%(data)s
output_format=mediumint signed

[functions]
matcher_data=lambda x: -8388608 <= int(x) <= 8388607
