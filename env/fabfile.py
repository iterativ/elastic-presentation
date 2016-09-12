# -*- coding: utf-8 -*-
#
# Iterativ GmbH
# http://www.iterativ.ch/
#
# Copyright (c) 2015 Iterativ GmbH. All rights reserved.
#
# Created on 16/10/15
# @author: pawel
import uuid
import decimal
from fabric.api import env
from fabric.context_managers import cd
from fabric.operations import sudo, run, get
from fabric.tasks import Task
from local.local import *


env.rsync_exclude.remove('*.dat')
env.rsync_exclude = env.rsync_exclude + ['media/']


class VenvTask(Task):

    def virtualenv(self, command):
        # env.run("source %s/bin/activate && python %s/%s" % (env.remote_virtualenv, env.project_home, command))
        env.run("source %s/bin/activate && python %s" % (env.remote_virtualenv, command))


class DumpTask(VenvTask):

    def dump(self, file_name, index_name, type_name):

        with cd(env.project_home):
            self.virtualenv('thor/dump.py -e %s -f %s -i %s -t %s' % (env.elastic_endpoint, file_name, index_name, type_name))

            # env.run('tar cvf %s.tar.gz ' % file_name)
            try:
                get('data/%s' % file_name, '../data/%s' % file_name)
            except:
                pass


class GitPull(Task):
    """
    Git pull to get up-to-date code
    """
    name = "git_pull"

    def run(self):
        with cd(env.project_home):
            env.run('git pull')


class DumpStats(DumpTask):
    """
    Dump usage stats
    """
    name = "dump_stats"

    def run(self):
        env.run('pwd')
        file_name = 'stats_dump.json'
        index_name = 'stats'
        type_name = 'entry'

        self.dump(file_name, index_name, type_name)


class DumpProducts(DumpTask):
    """
    Dump products
    """
    name = "dump_products"

    def run(self):
        file_name = 'prods_dump.json'
        index_name = 'all_products_spryker_read'
        type_name = 'product'

        self.dump(file_name, index_name, type_name)


git_pull = GitPull()
dump_stats = DumpStats()
dump_prods = DumpProducts()
