[transformer]
name=MySQL text
compatible_writers=mysql
regexp=^(?P<data>.*)$
formatter="%(data)s"
output_type=text
type_format=TEXT

[functions]
matcher_data=lambda x: len(x['data'])<65535
size=lambda x: len(x['data'])+2
