# -*- coding: utf-8 -*-
#
# Iterativ GmbH
# http://www.iterativ.ch/
#
# Copyright (c) 2015 Iterativ GmbH. All rights reserved.
#
# Created on 15/02/16
# @author: pawel
from optparse import OptionParser
import logging
import os
import pprint
import uuid

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk
import ijson
import sys

from common.json_helper import load_json_file

logger = logging.getLogger('main')


class DataIndexer:

    def handle(self, *args, **options):
        options = self.make_opts()

        # options.file_name
        # print 'Index Name: %s' % options.index_name
        # options.type_name
        # options.elastic_host
        # options.batch_size

        es = Elasticsearch(
            [options.elastic_host],
            # port=80,
            # use_ssl=True,
            # verify_certs=True,
            # ca_certs=certifi.where(),
        )

        print '-------------- Data Target ---------------'
        pprint.pprint(es.info())
        print '------------------------------------------'

        # 0 delete index
        try:
            print 'Deleting index'
            es.indices.delete(index=options.index_name)
        except Exception as ex:
            print 'Index doesn\'t exist yet'

        # 1 install mapping
        print 'Install mapping'
        mapping = load_json_file('../mappings/{}'.format(options.mapping_file))
        es.indices.create(index=options.index_name)
        es.indices.put_mapping(index=options.index_name, doc_type=options.type_name, body=mapping)

        # which data do we want to process
        # dataselector = (options.index_name, options.type_name)

        print 'Indexing into index:     %s' % options.index_name
        print 'Of type:                 %s' % options.type_name
        print 'In batches of:           %s' % options.batch_size
        print 'From file:               %s' % options.file_name
        print '------------------------------------------'

        # progress bar
        stdout_unbuf = os.fdopen(sys.stdout.fileno(), 'w', 0)
        # counter
        start = 0
        step = options.batch_size
        total = 100000

        for ok, result in streaming_bulk(es,
                                         self.godata(options.file_name),
                                         index=options.index_name,
                                         doc_type=options.type_name,
                                         chunk_size=options.batch_size):

            action, result = result.popitem()
            doc_id = result['_id']

            progress = float(min(total, start + step)) / total
            stdout_unbuf.write('\r[{0:50s}] {1:.1f}%'.format(
                '#' * int(progress * 50), progress * 100)
            )

            start += step

            # process the information from ES whether the document has been successfully indexed
            if not ok:
                print('Failed to %s document %s: %r' % (action, doc_id, result))

        print 'Number of processed records: %s' % (start / 100)

    def godata(self, file_name):
        with open(file_name) as dump_file:
            objects = ijson.items(dump_file, '')

            for otop in objects:
                for olow in otop:
                    olow['_id'] = str(uuid.uuid1())
                    #olow['_id'] = olow['gid']
                    yield olow

    def make_opts(self):
        parser = OptionParser()
        parser.add_option("-e", "--elasticsearch", dest="elastic_host", metavar="ELASTIC_HOST", help="elastic host")
        parser.add_option("-f", "--file", dest="file_name", metavar="FILE", help="write dump to FILE")
        parser.add_option("-m", "--mapping-file", dest="mapping_file", metavar="MAPPINGFILE", help="file with the elastic mapping")
        parser.add_option("-i", "--index", dest="index_name", metavar="INDEX", help="name of the index")
        parser.add_option("-t", "--type", dest="type_name", metavar="TYPE", help="name of the type")
        parser.add_option("-c", "--batch-size", action='store', type="int", dest='batch_size', default=100, help='size of the update batches'),

        (options, args) = parser.parse_args()
        return options


def main():
    indexer = DataIndexer()
    indexer.handle()


if __name__ == "__main__":
    main()
