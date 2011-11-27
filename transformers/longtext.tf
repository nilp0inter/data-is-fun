[transformer]
name=MySQL longtext
compatible_writers=mysql
regexp=^(?P<data>.*)$
formatter="%(data)s"
output_type=longtext
type_format=LONGTEXT

[functions]
matcher_data=lambda x: len(x['data'])<4294967295
size=lambda x: len(x['data'])+4
