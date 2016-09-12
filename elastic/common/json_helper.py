# -*- coding: utf-8 -*-
#
# Iterativ GmbH
# http://www.iterativ.ch/
#
# Copyright (c) 2016 Iterativ GmbH. All rights reserved.
#
# Created on 12/09/16
# @author: pawel
import json
import os


def load_json_file(json_file_path):
    """
    Loads a dict from a json file
    """
    base_dir = os.path.dirname(os.path.realpath(__file__))
    full_path = '{}/{}'.format(base_dir, json_file_path)
    return json.load(open(full_path))
