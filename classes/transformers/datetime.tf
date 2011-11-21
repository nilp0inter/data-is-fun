[transformer]
name=MySQL datetime
compatible_writers=mysql
regexp=^(?P<year>\d{4})-(?P<month>\d{1,2})-(?P<day>\d{1,2})\s+(?P<hour>\d{1,2}):(?P<minute>\d{1,2}):(?P<second>\d{1,2})$
formatter=datetime('%(year)s-%(month)s-%(day)s %(hour)s:%(minute)s:%(second)s')
output_type=datetime
type_format=DATETIME

[functions]
matcher_year=lambda x: int(x['year'])>=1000
matcher_month=lambda x: 1 <= int(x['month']) <= 12
matcher_day=lambda x: True if int(x['day'])<=28 else True if int(x['day'])==29 and int(x['month'])==2 and (True if int(x['year'])%4 == int(x['year'])%100 == int(x['year'])%400 == 0 or int(x['year'])%4 == 0 != int(x['year'])%100 else False) else True if int(x['day'])==30 and int(x['month'])!=2 and int(x['month'])%2==0 else True if int(x['day'])==31 and int(x['month'])%2 != 0 else False 
matcher_hour=lambda x: 0 <= int(x['hour']) <= 23
matcher_minute=lambda x: 0 <= int(x['minute']) <= 59
matcher_second=lambda x: 0 <= int(x['second']) <= 59
