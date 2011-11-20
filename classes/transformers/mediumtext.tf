[transformer]
name=MySQL mediumtext
regexp=^(?P<data>.*)$
formatter="%(data)s"
output_format=MEDIUMTEXT

[functions]
matcher_data=lambda x: len(x['data'])<16777215
size=lambda x: len(x['data'])+3
