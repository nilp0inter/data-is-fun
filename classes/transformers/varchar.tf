[transformer]
name=MySQL varchar
regexp=^(?P<data>.*)$
formatter="%(data)s"
output_format=VARCHAR(%s)

[functions]
matcher_data=lambda x: len(x['data'])<=255
size=lambda x: len(x['data'])+1
typesize=lambda x: [ len(x['data']) ]
