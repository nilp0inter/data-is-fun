[transformer]
name=DB2 Time
regexp=^(?P<hour>\d{2})\.(?P<minute>\d{2})\.(?P<second>\d{2})$
formatter=time('%(hour)s:%(minute)s:%(second)s')
output_format=TIME

[functions]
matcher_hour=lambda x: 0 <= int(x['hour']) <= 23
matcher_minute=lambda x: 0 <= int(x['minute']) <= 59
matcher_second=lambda x: 0 <= int(x['second']) <= 59
