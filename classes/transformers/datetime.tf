[transformer]
name=MySQL timestamp
regexp=^(?P<year>\d{4})-(?P<month>\d{1,2})-(?P<day>\d{1,2})\s+(?P<hour>\d{1,2}):(?P<minute>\d{1,2}):(?P<second>\d{1,2})$
formatter=datetime('%(year)s-%(month)s-%(day)s %(hour)s:%(minute)s:%(second)s')
output_format=datetime
