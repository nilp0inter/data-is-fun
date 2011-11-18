[transformer]
name=MySQL tinytext
regexp=^(?P<data>.*)$
formatter="%(data)s"
output_format=tinytext

[functions]
matcher_data=lambda x: len(x)<255
