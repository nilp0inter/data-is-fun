[transformer]
name=MySQL tinytext
regexp=^(?P<data>.*)$
formatter="%(data)s"
output_format=TINYTEXT

[functions]
matcher_data=lambda x: len(x['data'])<=255
