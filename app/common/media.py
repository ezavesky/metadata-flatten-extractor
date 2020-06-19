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

# Imports
import subprocess
from pathlib import Path

import logging
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

_FFMPEG_VALID = None

def clip_video(media_file, media_output, start, duration=1, image_only=False):
    """Helper function to create video clip"""
    global _FFMPEG_VALID
    path_media = Path(media_output)
    if path_media.exists():
        path_media.unlink()

    if _FFMPEG_VALID is None:
        # modified for subprocess - https://stackoverflow.com/a/16516701 
        try:
            proc = subprocess.Popen(["ffmpeg"], shell=False, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            _FFMPEG_VALID = True
        except FileNotFoundError as e:
            _FFMPEG_VALID = False
    if not _FFMPEG_VALID:   #  tested and not available, quit now
        return -1

    #detect the crop in the first 2 minutes
    cmd_list = ["ffmpeg", "-ss", str(start), "-i", media_file, "-y"]
    if not image_only:
        cmd_list += ["-t", str(duration), "-c", "copy", media_output]
    else: 
        cmd_list += ["-t", "1", "-r", "1", "-f", "image2", media_output]
        # TODO: do we allow force of an aspect ratio for bad video transcode?  e.g. -vf 'scale=640:360' 

    # modified for subprocess - https://stackoverflow.com/a/16516701 
    proc = subprocess.Popen(cmd_list, shell=False, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    
    # Read line from stdout, break if EOF reached, append line to output
    list_results = []
    for line in proc.stdout:
        line = line.decode().strip()
        if "Stream #0:" in line:
            list_results.append(line)
    logger.info(f"Source: {media_file}; Destination: (time: {start}s, duration: {duration}s) -> Output: {media_output} [[{list_results}]]")
    return 0 if path_media.exists() else -1
