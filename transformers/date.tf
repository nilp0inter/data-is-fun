[transformer]
name=MySQL date
compatible_writers=mysql
regexp=^(?P<data>\d{4}-\d{1,2}-\d{1,2})$
formatter=date('%(data)s')
output_type=date
type_format=DATE
