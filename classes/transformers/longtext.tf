[transformer]
name=MySQL longtext
regexp=^(?P<data>.*)$
formatter="%(data)s"
output_format=longtext

[functions]
matcher_data=lambda x: len(x)<4294967295
