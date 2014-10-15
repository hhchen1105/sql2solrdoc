# rename this file to field_mapping.py

import collections

# TODO: change the following table name
sql_table_name = 'sql-table-name'

field_mapping = collections.OrderedDict([
    # TODO: change the following fields
    # key: the column name in sql; value: the field name and field type in Solr doc
    # format: ('column-name-in-sql', ('field-name-in-solr-doc', 'field-type-in-solr-doc'))
    ('column1', ('id', 'string')),
    ('column2', ('title', 'text_general')),
    ('column3', ('price', 'int')),
])
