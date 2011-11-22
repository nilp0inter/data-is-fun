[transformer]
name=MySQL char
compatible_writers=mysql
regexp=^(?P<data>.*)$
formatter="%(data)s"
output_type=char
type_format=CHAR(%s)

[functions]
matcher_data=lambda x: len(x['data'])<=255
size=lambda x: len(x['data']) if not x['_last_type_size'] else int(x['_last_type_size'][0])
typesize=lambda x: [ len(x['data']) ]
