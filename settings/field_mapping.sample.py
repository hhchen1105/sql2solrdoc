# rename this file to field_mapping.py

field_mapping = {
    # TODO: change the following fields
    # key: the column name in sql; value: the field name in Solr doc
    'column1': ('id', 'string'),
    'column2': ('title', 'text_general'),
    'column3': ('price', 'int'),
}
