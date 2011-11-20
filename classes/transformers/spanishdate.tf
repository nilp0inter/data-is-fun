[transformer]
name=Spanish date format
# 9/6/85
regexp=^(?P<day>\d{1,2})/(?P<month>\d{1,2})/(?P<year>\d{1,4})$
formatter=date('%(year)s-%(month)s-%(day)s')
output_format=DATE

[functions]
matcher_day=lambda x: int(x['day'])>=1 and int(x['day'])<=31
matcher_month=lambda x: int(x['day'])>=1 and int(x['day'])<=12

preformat_day=lambda x: x['day'].rjust(2,'0') 

preformat_month=lambda x: x['month'].rjust(2,'0')
preformat_year=lambda x: x['year'].rjust(4, '0') if int(x['year'])>99 else str(int(x['year'])+2000) if int(x['year'])<10 else str(int(x['year'])+1900)

post_format=lambda x,y: int(y['day'])*x # so fun!
