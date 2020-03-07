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


# extract __version__ from version file. importing will lead to install failures
globals_dict = dict()
setup_dir = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(setup_dir, 'metadata_flatten', '_version.py')) as file:
    exec(file.read(), globals_dict)

# get the dependencies and installs
print(setup_dir)
with open(os.path.join(setup_dir, 'requirements.txt'), encoding='utf-8') as f:
    all_reqs = f.read().split('\n')
requirement_list = [x.strip() for x in all_reqs if 'git+' not in x and not x.startswith('#')]

setup(
    name=globals_dict['__package__'],
    version=globals_dict['__version__'],
    packages=find_packages(),
    author=globals_dict['__author__'],
    description=globals_dict['__description__'],
    long_description=(globals_dict['__description__']),
    license="Apache",
    package_data={globals_dict['__package__']: ['data/*']},
    # setup_requires=['pytest-runner'],
    entry_points="""
    [console_scripts]
    metadata-flatten=metadata_flatten:main
    """,
    # setup_requires=['pytest-runner'],
    python_requires='>=3.6',
    install_requires=requirement_list,
    tests_require=[],
    # cmdclass={'install': new_install},
    include_package_data=True,
)