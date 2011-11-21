[transformer]
name=MySQL time
compatible_writers=mysql
regexp=(?P<sign>(?:\+|-)?)(?P<hour>\d{1,3}):(?P<minute>\d{1,2}):(?P<second>\d{1,2})
formatter=time('%(sign)s%(hour)s:%(minute)s:%(second)s')
output_type=time
type_format=TIME

[functions]
matcher_hour=lambda x: int(x['hour']) <= 838 

preformat_sign=lambda x: "" if x == None else x['sign']
