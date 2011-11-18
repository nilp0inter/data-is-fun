[transformer]
name=MySQL varchar
regexp=^(?P<data>.*)$
formatter="%(data)s"
output_format=varchar(255)

[functions]
matcher_data=lambda x: len(x)<255
