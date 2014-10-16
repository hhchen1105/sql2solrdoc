#!/usr/bin/env python

# Hung-Hsuan Chen <hhchen@psu.edu>
# Creation Date : 12-19-2012
# Last Modified: Thu 16 Oct 2014 09:41:01 AM CST

import os
import sys

from lxml import etree

import mysql_util
import settings.field_mapping


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


def create_solr_doc_files():
    batchsize = 100000
    solr_file_folder = "./output_solr_files"
    table_name = settings.field_mapping.sql_table_name
    field_mapping = settings.field_mapping.field_mapping

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
                else:
                    raise Exception('\nUndefined field type "%s"' % (field_type))
                doc.append(ele)
            root.append(doc)
        print ''
        if not os.path.isdir(solr_file_folder):
                os.mkdir(solr_file_folder)
        f = open(os.path.join(solr_file_folder, 'products%d.xml' % (num_files)), 'w')
        tree = etree.ElementTree(root)
        tree.write(f, encoding='utf8', pretty_print=True)
        f.close()

    mysql_util.close_db(db, cursor)


def main(argv):
    create_solr_doc_files()


if __name__ == "__main__":
    main(sys.argv)

