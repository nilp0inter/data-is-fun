[transformer]
name=MySQL text
regexp=^(?P<data>.*)$
formatter="%(data)s"
output_format=TEXT

[functions]
matcher_data=lambda x: len(x['data'])<65535
size=lambda x: len(x['data'])+2
