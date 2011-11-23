[transformer]
name=MySQL mediumtext
compatible_writers=mysql
regexp=^(?P<data>.*)$
formatter="%(data)s"
output_type=mediumint
type_format=MEDIUMTEXT

[functions]
matcher_data=lambda x: len(x['data'])<16777215
size=lambda x: len(x['data'])+3
