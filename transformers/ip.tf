[transformer]
name=IPv4 as unsigned int
compatible_writers=mysql
regexp=^(?P<data>(?P<oct1>\d{1,3})\.(?P<oct2>\d{1,3})\.(?P<oct3>\d{1,3})\.(?P<oct4>\d{1,3}))$
formatter=inet_aton("%(data)s")
output_type=unsigned_int
type_format=INT UNSIGNED

[functions]
matcher_oct1=lambda x: 0 <= int(x['oct1']) <= 255
matcher_oct2=lambda x: 0 <= int(x['oct2']) <= 255
matcher_oct3=lambda x: 0 <= int(x['oct3']) <= 255
matcher_oct4=lambda x: 0 <= int(x['oct4']) <= 255

size=lambda x: 4
