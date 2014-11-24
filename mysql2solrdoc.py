#!/usr/bin/env python

# Hung-Hsuan Chen <hhchen@psu.edu>
# Creation Date : 12-19-2012
# Last Modified: Mon 24 Nov 2014 08:36:50 PM CST

import os
import sys

import gflags

import mysql_util
import settings.field_mapping

from lxml import etree


FLAGS = gflags.FLAGS
gflags.DEFINE_string('solr_doc_filename_prefix', '', '')

def usage(cmd):
    print ('Usage:', cmd,
            '--solr_doc_filename_prefix="product"')
    return


def check_args(argv):
    try:
        argv = FLAGS(argv)
    except gflags.FlagsError:
        print FLAGS

    if FLAGS.solr_doc_filename_prefix == '':
        usage(argv[0])
        raise Exception('flag --solr_doc_filename_prefix cannot be empty')


def valid_XML_char_ordinal(i):
    return ( # conditions ordered by presumed frequency
            0x20 <= i <= 0xD7FF 
            or i in (0x9, 0xA, 0xD) 
            or 0xE000 <= i <= 0xFFFD 
            or 0x10000 <= i <= 0x10FFFF
    )


def assign_xml_node_text(node, text):
    try:
        node.text = text.encode('utf8') if text is not None else ''
    except:
        node.text = ''.join(c for c in text if valid_XML_char_ordinal(ord(c))).decode('utf8')


def create_solr_doc_files(table_name, field_mapping, solr_file_folder, solr_doc_filename_prefix):
    batchsize = 100000

    db, cursor = mysql_util.init_db()
    print 'Querying database'
    sql = 'SELECT %s FROM %s' % (','.join(field_mapping.keys()), table_name)

    cursor.execute(sql)
    num_files = 0
    while True:
        rows = cursor.fetchmany(batchsize)
        if not rows:
            break
        num_files += 1
        sys.stdout.write('Generating doc file number %d\n' % num_files)
        root = etree.Element('add')
        
        for i, r in enumerate(rows):
            sys.stdout.write("\r%d / %d" % (i+1, len(rows)))
            doc = etree.Element('doc')
            for j, col in enumerate(r):
                (field_name, field_type) = field_mapping.values()[j]
                ele = etree.Element("field", name=field_name)
                if field_type in ['string', 'text_general']:
                    assign_xml_node_text(ele, col) if col is not None else ''
                elif field_type in ['int', 'float', 'long', 'double']:
                    ele.text = str(col) if col is not None else '0'
                elif field_type in ['date']:
                    ele.text = col.strftime("%Y-%m-%dT%H:%M:%SZ") if col is not None else ''
                elif field_type in ['boolean']:
                    ele.text = 'true' if col else 'false'
                else:
                    raise Exception('\nUndefined field type "%s"' % (field_type))
                doc.append(ele)
            root.append(doc)
        print ''
        if not os.path.isdir(solr_file_folder):
            os.mkdir(solr_file_folder)
        f = open(os.path.join(solr_file_folder, '%s%d.xml' % (solr_doc_filename_prefix, num_files)), 'w')
        tree = etree.ElementTree(root)
        tree.write(f, encoding='utf8', pretty_print=True)
        f.close()

    mysql_util.close_db(db, cursor)


def create_partial_solr_schema(field_mapping, solr_file_folder):
    with open(os.path.join(solr_file_folder, 'schema.xml'), 'w') as f:
        for (field_name, field_type) in field_mapping.values():
            f.write('<field name="%s" type="%s" indexed="true" stored="true" required="true" multiValued="false"/>\n'
                    % (field_name, field_type))

        for (field_name, field_type) in field_mapping.values():
            f.write('<copyField source="%s" dest="text"/>\n' % (field_name))


def main(argv):
    check_args(argv)

    table_name = settings.field_mapping.sql_table_name
    field_mapping = settings.field_mapping.field_mapping
    solr_file_folder = "./output_solr_files"

    create_solr_doc_files(table_name, field_mapping, solr_file_folder, FLAGS.solr_doc_filename_prefix)
    create_partial_solr_schema(field_mapping, solr_file_folder)


if __name__ == "__main__":
    main(sys.argv)


