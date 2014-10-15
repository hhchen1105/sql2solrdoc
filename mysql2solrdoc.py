#!/usr/bin/env python

# Hung-Hsuan Chen <hhchen@psu.edu>
# Creation Date : 12-19-2012
# Last Modified: Wed 15 Oct 2014 03:52:42 PM CST

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
    solr_file_folder = "./solr_files"
    table_name = settings.field_mapping.sql_table_name
    field_mapping = settings.field_mapping.field_mapping

    db, cursor = mysql_util.init_db()
    print 'Querying database'
    sql = 'SELECT %s FROM %s' % (','.join(field_mapping.keys()), table_name)
    cursor.execute(sql)

    # TODO: start from here
    num_files = 0
    while True:
        rows = cursor.fetchmany(batchsize)
        if not rows:
            break
        num_files += 1
        sys.stdout.write('Generating doc file number %d\n' % num_files)
        root = etree.Element('add')
        num_docs_to_gen = len(rows)
        for i, r in enumerate(rows):
            sys.stdout.write("\r%d / %d, GOODS_CODE: %s" % (i+1, num_docs_to_gen, r[0]))
            doc = etree.Element('doc')
            goods_code = etree.Element("field", name="GOODS_CODE")
            goods_code.text = str(r[0])
            goods_name = etree.Element("field", name="GOODS_NAME")
            assign_xml_node_text(goods_name, r[1]) if r[1] is not None else ''
            keyword = etree.Element("field", name="KEYWORD")
            assign_xml_node_text(keyword, r[2]) if r[2] is not None else ''
            brand_name = etree.Element("field", name="BRAND_NAME")
            assign_xml_node_text(brand_name, r[3]) if r[3] is not None else ''
            sale_price = etree.Element("field", name="SALE_PRICE")
            sale_price.text = str(r[4]) if r[4] is not None else '0'
            describe_301 = etree.Element("field", name="DESCRIBE_301")
            assign_xml_node_text(describe_301, r[5]) if r[5] is not None else ''
            describe_302 = etree.Element("field", name="DESCRIBE_302")
            assign_xml_node_text(describe_302, r[6]) if r[6] is not None else ''
            doc.append(goods_code)
            doc.append(goods_name)
            doc.append(keyword)
            doc.append(brand_name)
            doc.append(sale_price)
            doc.append(describe_301)
            doc.append(describe_302)
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

