[transformer]
name=MySQL varchar
compatible_writers=mysql
regexp=^(?P<data>.*)$
formatter="%(data)s"
output_type=varchar
type_format=VARCHAR(%s)

[functions]
matcher_data=lambda x: len(x['data'])<=255
size=lambda x: len(x['data'])+1 if type(x['data']) == str() else len(x['data'])+2
typesize=lambda x: [ len(x['data']) ]
