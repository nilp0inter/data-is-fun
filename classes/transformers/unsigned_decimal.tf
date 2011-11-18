[transformer]
name=MySQL unsigned decimal
regexp=^(?P<all>(?P<decimal>\d+)(:?\.(?P<mantissa>\d+))?)$
formatter=%(decimal)s.%(mantissa)s
output_format=decimal unsigned

[functions]
pre_format_mantissa=lambda x: "0" if x['mantissa'] == None else x['mantissa']
matcher_all=lambda x: len(x.replace('-.',''))<=65
matcher_mantissa=lambda x: len(x)<=30
