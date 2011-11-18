[transformer]
name=MySQL signed decimal
regexp=^(?P<all>(?P<sign>\+|-)?(?P<decimal>\d+)(:?\.(?P<mantissa>\d+))?)$
formatter=%(sign)s%(decimal)s.%(mantissa)s
output_format=decimal signed

[functions]
pre_format_mantissa=lambda x: "0" if x['mantissa'] == None else x['mantissa']
pre_format_sign=lambda x: "" if x['sign'] == None else x['sign']
matcher_all=lambda x: len(x.replace('-.',''))<=65
matcher_mantissa=lambda x: len(x)<=30
