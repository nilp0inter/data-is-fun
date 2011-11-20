[transformer]
name=MySQL unsigned decimal
regexp=^(?P<all>(?P<decimal>\d+)(:?\.(?P<mantissa>\d+))?)$
formatter=%(decimal)s.%(mantissa)s
output_format=DECIMAL(%s) UNSIGNED

[functions]
matcher_all=lambda x: len(x['all'].replace('-.',''))<=65
matcher_mantissa=lambda x: len(x['mantissa'])<=30

preformat_mantissa=lambda x: "0" if x['mantissa'] == None else x['mantissa']

typesize=lambda x: [ len(x['decimal']+("0" if x['mantissa'] == None else x['mantissa'])), 0 if x['mantissa'] == None else len(x['mantissa']) ]
