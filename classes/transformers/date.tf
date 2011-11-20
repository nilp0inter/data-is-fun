[transformer]
name=MySQL date
regexp=^(?P<data>\d{4}-\d{1,2}-\d{1,2})$
formatter=date('%(data)s')
output_format=DATE
