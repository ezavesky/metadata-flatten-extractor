#! python
# ===============LICENSE_START=======================================================
# metadata-flatten-extractor Apache-2.0
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
# -*- coding: utf-8 -*-

import subprocess
from pathlib import Path


def static_symlink(path_target, name_link, package_name="streamlit"):
    # run show command to determine where package is (see https://stackoverflow.com/a/46071447)
    cmd_list = ["pip", "show", package_name]  
    # modified for subprocess - https://stackoverflow.com/a/16516701 (5/27)
    proc = subprocess.Popen(cmd_list, shell=False, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    list_results = []
    list_raw = []
    # Read line from stdout, break if EOF reached, append line to output
    for line in proc.stdout:
        line = line.decode().strip()
        if "Location" in line:
            line_parts = line.split(" ")
            if len(line_parts) == 2:  # slightly odd, but nested 'static' entries for true static hosting
                list_results.append(Path(line_parts[-1]).joinpath(package_name, 'static', 'static'))
        list_raw.append(line)
    if len(list_results) == 0:
        print(f"Error, could not find package location (source {list_raw})")
        return None

    # create target directory if it doesn't exist (all permissions)
    path_target = Path(path_target)
    if not path_target.exists():
        path_target.mkdir(parents=True)

    # create symlink directory to target with specific name
    path_link = list_results[0].joinpath(name_link)
    if path_link.exists():
        print(f"Symlink {str(path_link)} already exists, skipping recreate")
    else:
        path_link.symlink_to(path_target, True)
    return str(path_link)


def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description="""A script to create a static/symlink within streamlit's repo.""",
        epilog="""Application examples
            # specify a link with the name (streamlit-tmp) 
            python static_link_create --name streamlit-tmp
            # specify a link to a new target (/Desktop) 
            python static_link_create --target /Desktop 
    """, formatter_class=argparse.RawTextHelpFormatter)
    submain = parser.add_argument_group('main execution')
    submain.add_argument('-n', '--name', dest='name', type=str, default='dynamic', help='specify the name for the directory to be created')
    submain.add_argument('-t', '--target', dest='target', type=str, default='/tmp', help='specify the target of the symlink directory')
    config = vars(parser.parse_args())
    str_link = static_symlink(config['target'], config['name'])
    print(f"Link created at {str_link}...")


# main block run by code
if __name__ == '__main__':
    main()