# -*- coding: utf-8 -*-
#
# Iterativ GmbH
# http://www.iterativ.ch/
#
# Copyright (c) 2015 Iterativ GmbH. All rights reserved.
#
# Created on 09/02/16
# @author: pawel
from fabric.api import env

from deployit.fabrichelper.environments import EnvTask
from fabric.operations import local
from fabric.tasks import Task


class LocalEnv(EnvTask):
    """
    Use Production environment
    """
    name = "local"

    def run(self):
        env.hosts = ['localhost']
        env.project_name = 'searchService'
        env.project_home = '/Users/pawel/workspace/elastic-presentation/'
        env.remote_virtualenv = '/Users/pawel/workspace/elastic-presentation-env'

        env.search_service_endpoint = 'http://localhost:5000'
        env.elastic_endpoint = 'http://localhost:9200'
        env.elastic_proxy = env.elastic_endpoint

        # hack to have the commands available locally aswell
        env.run = local


class IndexData(Task):
    """
    Index products
    """
    name = "index_albums"

    def run(self):
        local('python ../elastic/index_bulk.py -e %s -f %s -m ../elastic/mappings/albums.json -i %s -t %s -c 100' % ('data.iterativ.ch:9200', '../data/musicbrainz.json', 'albums', 'album'))


local_env = LocalEnv()
index_data = IndexData()
