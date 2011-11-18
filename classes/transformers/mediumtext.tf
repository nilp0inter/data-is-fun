[transformer]
name=MySQL mediumtext
regexp=^(?P<data>.*)$
formatter="%(data)s"
output_format=mediumtext

[functions]
matcher_data=lambda x: len(x)<16777215
