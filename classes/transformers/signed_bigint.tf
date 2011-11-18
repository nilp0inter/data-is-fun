[transformer]
name=MySQL signed bigint
regexp=^(?P<data>-?\d+)$
formatter=%(data)s
output_format=bigint signed

[functions]
matcher_data=lambda x: -9223372036854775808 <= int(x) <= 9223372036854775807
