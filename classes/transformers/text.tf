[transformer]
name=MySQL text
regexp=^(?P<data>.*)$
formatter="%(data)s"
output_format=text

[functions]
matcher_data=lambda x: len(x)<65535
