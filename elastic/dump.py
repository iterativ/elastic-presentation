# -*- coding: utf-8 -*-
#
# Iterativ GmbH
# http://www.iterativ.ch/
#
# Copyright (c) 2015 Iterativ GmbH. All rights reserved.
#
# Created on 05/10/15
# @author: pawel
import json
from optparse import OptionParser
import os
import pprint

from elasticsearch import Elasticsearch


def main():
    options = make_opts()

    # options.file_name
    # options.index_name
    # options.type_name
    # options.elastic_host

    es = Elasticsearch(
        [options.elastic_host],
        # port=80,
        # use_ssl=True,
        # verify_certs=True,
        # ca_certs=certifi.where(),
    )

    print '---- Data Target ----'
    pprint.pprint(es.info())
    print '---------------'

    # which data do we want to process
    dataselector = (options.index_name, options.type_name)

    rs = es.search(index=dataselector[0],
                   scroll='10s',
                   search_type='scan',
                   size=10000,
                   body={
                       "query": {
                           "match_all": {}
                       }
                   })

    sid = rs['_scroll_id']
    print sid

    with open('data/%s' % options.file_name, 'w') as dump_file:
        dump_file.write('[')

        try:
            while True:
                print '.'
                rs = es.scroll(scroll_id=sid, scroll='10s')
                hits = rs['hits']['hits']
                for d in hits:
                    dump_file.write(json.dumps(d['_source']))
                    dump_file.write(',')
        except:
            pass

        # dirty hack to remove last comma
        dump_file.seek(-1, os.SEEK_END)
        dump_file.truncate()

        # close arr
        dump_file.write(']')


def make_opts():
    parser = OptionParser()
    parser.add_option("-e", "--elasticsearch", dest="elastic_host", metavar="ELASTIC_HOST", help="elastic host")
    parser.add_option("-f", "--file", dest="file_name", metavar="FILE", help="write dump to FILE")
    parser.add_option("-i", "--index", dest="index_name", metavar="INDEX", help="name of the index")
    parser.add_option("-t", "--type", dest="type_name", metavar="TYPE", help="name of the type")

    (options, args) = parser.parse_args()
    return options


main()
