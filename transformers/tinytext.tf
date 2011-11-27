[transformer]
name=MySQL tinytext
compatible_writers=mysql
regexp=^(?P<data>.*)$
formatter="%(data)s"
output_type=tinytext
type_format=TINYTEXT

[functions]
matcher_data=lambda x: len(x['data'])<=255
size=lambda x: len(x['data'])+1
