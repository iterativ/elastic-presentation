# -*- coding: utf-8 -*-
#
# Iterativ GmbH
# http://www.iterativ.ch/
#
# Copyright (c) 2015 Iterativ GmbH. All rights reserved.
#
# Created on 05/10/15
# @author: pawel
from optparse import OptionParser
import pprint

from elasticsearch import Elasticsearch
import ijson


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
        request_timeout=1000
    )

    print '---- Data Target ----'
    pprint.pprint(es.info())
    print '---------------'

    # which data do we want to process
    dataselector = (options.index_name, options.type_name)

    with open(options.file_name) as dump_file:
        objects = ijson.items(dump_file, '')

        for otop in objects:
            for olow in otop:
                try:
                    es.index(index=dataselector[0], doc_type=dataselector[1], body=olow)

                except Exception as e:
                    pass


def make_opts():
    parser = OptionParser()
    parser.add_option("-e", "--elasticsearch", dest="elastic_host", metavar="ELASTIC_HOST", help="elastic host")
    parser.add_option("-f", "--file", dest="file_name", metavar="FILE", help="write dump to FILE")
    parser.add_option("-i", "--index", dest="index_name", metavar="INDEX", help="name of the index")
    parser.add_option("-t", "--type", dest="type_name", metavar="TYPE", help="name of the type")

    (options, args) = parser.parse_args()
    return options


main()
