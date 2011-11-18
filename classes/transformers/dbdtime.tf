[transformer]
name=DB2 Time
regexp=^(?P<hour>\d{2})\.(?P<minute>\d{2})\.(?P<second>\d{2})$
formatter=time('%(hour)s:%(minute)s:%(second)s')
output_format=time
