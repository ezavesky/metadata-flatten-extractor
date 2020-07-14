# -*- coding: utf-8 -*-
#! python
# ===============LICENSE_START=======================================================
# scene-me Apache-2.0
# ===================================================================================
# Copyright (C) 2017-2020 AT&T Intellectual Property. All rights reserved.
# ===================================================================================
# This software file is distributed by AT&T 
# under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# This file is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ===============LICENSE_END=========================================================


import argparse
import logging
import sys
from os.path import dirname, abspath, join as path_join, exists
from os import makedirs

import logging

logger = logging.getLogger()

# formatter = logging.Formatter(fmt='%(asctime)s %(name)-12s %(levelname)-8s %(message)s', datefmt='%m-%d %H:%M')
# handler = logging.StreamHandler()
# handler.setFormatter(formatter)
# logger.handlers = []
# logger.addHandler(handler)
# logger.propagate = False

from flask import request, render_template, send_from_directory, Flask, make_response

_ROOT = dirname(dirname(abspath(__file__)))

if __name__ == '__main__':
    # patch the path to include this object
    if _ROOT not in sys.path:
        sys.path.append(_ROOT)
    # monkeypatch to import metadata_flatten package
    try:
        from metadata_flatten import parsers
    except Exception as e:
        _PACKAGE = dirname(_ROOT)
        logger.warning(f"Force-import metadata package... {_PACKAGE}")
        if _PACKAGE not in sys.path:
            sys.path.append(_PACKAGE)


from index import callback_create, layout_generate, create_dash_app, get_dash_app, models_load, dataset_load


### ------------------------------------------------
###  --- Gunicorn Management and App Creation ----
### ------------------------------------------------


def create_app(argv=sys.argv[1:]):
    version_path = path_join("..", "..", "metadata_flatten", "_version.py")
    version_data = {}
    with open(version_path) as file:
        exec(file.read(), version_data)
    app_title = "Lexicon Mapper"

    parser = argparse.ArgumentParser(
        description=app_title
    )
    parser.add_argument("-p", "--port", type=int, default=8080, help="Port for HTTP server (default: 8080)")
    parser.add_argument("-z", "--result_count", type=int, default=30, help="Max results per page")
    parser.add_argument("-l", "--log_size", type=int, default=200, help="Max log length for on-screen display")
    parser.add_argument("-r", "--refresh_interval", type=int, default=2000, help="Refresh interval for log (in millis)")
    parser.add_argument("--verbose", "-v", action="count")
    subparse = parser.add_argument_group('media configuration')
    subparse.add_argument("--data_dir", type=str, default='model', help="specify the source directory for model")
    subparse.add_argument('-n', '--mapping_model', dest='mapping_model', type=str, default='en_core_web_lg', 
        help='spacy mapping model if NLP models installed - https://spacy.io/models')

    run_settings = vars(parser.parse_args(argv))
    logger.info(f"Run Settings: {run_settings}")

    if not exists(run_settings["data_dir"]):
        makedirs(run_settings["data_dir"])
        if not exists(run_settings["data_dir"]):
            logger.fatal(f"Attempt to create {run_settings['data_dir']} failed, please check filesystem permissions")
            return None

    if run_settings['verbose']:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    server = Flask(__name__) # define flask app.server
    app_obj = create_dash_app(__name__, server, run_settings['log_size'])   # NOTE: this updates _app_obj
    app_obj.models = models_load(run_settings['mapping_model'], run_settings['data_dir'])
    app_obj.dataset = dataset_load(run_settings['data_dir'])

    app_obj.title = app_title
    app_obj.logger = logger
    app_obj.settings = run_settings
    app_obj.layout = layout_generate

    app_obj.config.suppress_callback_exceptions = True
    
    callback_create(app_obj)

    # static for serving local media
    STATIC_PATH = path_join(_ROOT, 'assets', 'media')
    @app_obj.server.route('/media/<resource>')
    def serve_static(resource):
        return send_from_directory(STATIC_PATH, resource)

    @app_obj.server.route('/api/domain', methods=['GET'])
    def domain_list(domain, query):
        return make_response("{}")
        # return send_from_directory(STATIC_PATH, resource)

    @app_obj.server.route('/api/domain/<domain>', methods=['POST'])
    def domain_create(domain):
        return make_response("{}")


    @app_obj.server.route('/api/map/<domain>/<query>', methods=['GET', 'POST'])
    def map(domain, query):
        return make_response("{}")

    return server


# Gunicorn entry point generator, https://stackoverflow.com/a/46333363
def app(*args, **kwargs):
    # Gunicorn CLI args are useless.
    # https://stackoverflow.com/questions/8495367/
    #
    # Start the application in modified environment.
    # https://stackoverflow.com/questions/18668947/
    #
    argv = list(args)
    for k in kwargs:
        argv.append("--" + k)
        argv.append(kwargs[k])
    logger.info(f"App Settings: {argv}")
    return create_app(argv)


def main():
    global _app_obj
    
    create_app()
    app_obj = get_dash_app()
    # listener_attach(_app_obj)
    app_obj.run_server(host='0.0.0.0', port=app_obj.settings['port'], debug=True)

    # app_inst.run(host='0.0.0.0', port=app_inst.settings['port'])


if __name__ == "__main__":
    main()
