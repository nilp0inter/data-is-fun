[transformer]
name=MySQL longtext
regexp=^(?P<data>.*)$
formatter="%(data)s"
output_format=LONGTEXT

[functions]
matcher_data=lambda x: len(x['data'])<4294967295
size=lambda x: len(x['data'])+4
