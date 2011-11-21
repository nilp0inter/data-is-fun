[transformer]
name=MySQL signed decimal
compatible_writers=mysql
regexp=^(?P<all>(?P<sign>\+|-)?(?P<decimal>\d+)(:?\.(?P<mantissa>\d+))?)$
formatter=%(sign)s%(decimal)s.%(mantissa)s
output_type=decimal
type_format=DECIMAL(%s)

[functions]
matcher_all=lambda x: len(x['all'].replace('-.',''))<=65
matcher_mantissa=lambda x: len(x['mantissa'])<=30

preformat_mantissa=lambda x: "0" if x['mantissa'] == None else x['mantissa']
preformat_sign=lambda x: "" if x['sign'] == None else x['sign']

size=lambda x: int((len(x['decimal']+("0" if x['mantissa'] == None else x['mantissa']))-len(("0" if x['mantissa'] == None else x['mantissa'])))/9)*4+int((((len(x['decimal']+("0" if x['mantissa'] == None else x['mantissa']))-len(("0" if x['mantissa'] == None else x['mantissa'])))%9)+1)/2)+int(len(("0" if x['mantissa'] == None else x['mantissa']))/9)*4+int(((len(("0" if x['mantissa'] == None else x['mantissa']))%9)+1)/2)

typesize=lambda x: [ len(x['decimal']+("0" if x['mantissa'] == None else x['mantissa'])), 0 if x['mantissa'] == None else len(x['mantissa']) ]
