[transformer]
name=MySQL time
regexp=(?P<hour>\d{1,2}):(?P<minute>\d{1,2}):(?P<second>\d{1,2})
formatter=time('%(hour)s:%(minute)s:%(second)s')
output_format=time
