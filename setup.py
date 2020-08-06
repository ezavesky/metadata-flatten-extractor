#! python
# ===============LICENSE_START=======================================================
# vinyl-tools Apache-2.0
# ===================================================================================
# Copyright (C) 2017-2019 AT&T Intellectual Property. All rights reserved.
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
# -*- coding: utf-8 -*-
import os
from setuptools import setup, find_packages
from setuptools.command.install import install
from pathlib import Path


# extract __version__ from version file. importing will lead to install failures
globals_dict = dict()
setup_dir = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(setup_dir, 'contentai_metadata_flatten', '_version.py')) as file:
    exec(file.read(), globals_dict)

# reading string for concatenation
doc_string = ''
for doc_files in ["README.rst", "CHANGES.rst"]:
    with open(os.path.join(setup_dir, 'docs', doc_files), 'rt') as file:
       doc_string += file.read()  + "\n"

# get the dependencies and installs
print(setup_dir)
with open(os.path.join(setup_dir, 'requirements.txt'), encoding='utf-8') as f:
    all_reqs = f.read().split('\n')
requirement_list = [x.strip() for x in all_reqs if 'git+' not in x and not x.startswith('#') and x]

with open(os.path.join(setup_dir, 'testing', 'tox-requirements.txt'), encoding='utf-8') as f:
    all_reqs = f.read().split('\n')
test_requirement_list = [x.strip() for x in all_reqs if 'git+' not in x and not x.startswith('#') and x]

# collect data files and templates
data_dir = os.path.join(setup_dir, globals_dict['__package__']) + os.path.sep
# list_data = [str(x.resolve()).replace(data_dir, '') for x in Path(os.path.join(data_dir, 'data')).rglob('*.*')]
list_data = [str(x.resolve()) for x in Path(os.path.join(data_dir, 'data')).rglob('*.*')]

setup(
    name=globals_dict['__package__'],
    version=globals_dict['__version__'],
    packages=find_packages(),
    author=globals_dict['__author__'],
    description=globals_dict['__description__'],
    long_description=doc_string,
    long_description_content_type="text/x-rst",
    url='https://gitlab.research.att.com/turnercode/metadata-flatten-extractor',
    license="Apache",
    package_data={globals_dict['__package__']: list_data },
    # setup_requires=['pytest-runner'],
    entry_points="""
    [console_scripts]
    contentai-metadata-flatten=contentai_metadata_flatten.main:main
    """,
    python_requires='>=3.6',
    install_requires=requirement_list,
    tests_require=test_requirement_list,
    # cmdclass={'install': new_install},
    include_package_data=True,
)
