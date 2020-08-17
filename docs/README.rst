metadata-flatten-extractor
==========================

A method to flatten generated JSON data into timed CSV events in support
of analytic workflows within the `ContentAI
Platform <https://www.contentai.io>`__, published as the extractor
``dsai_metadata_flatten``.   There is also a 
`pypi package <https://pypi.org/project/contentai-metadata-flatten/>`__ 
of this package published for easy incorporation in other projects.

1. `Getting Started <#getting-started>`__
2. `Execution <#execution-and-deployment>`__
3. `Testing <#testing>`__
4. `Future Development <#future-development>`__
5. `Changes <#changes>`__

Getting Started
===============

| This library is used as a `single-run executable <#contentai-standalone>`__.
| Runtime parameters can be passed for processing that configure the
  returned results and can be examined in more detail in the
  `main <main.py>`__ script.

**NOTE: Not all flattening functions will respect/obey properties
defined here.**

-  ``force_overwrite`` - *(bool)* - force existing files to be
   overwritten (*default=False*)
-  ``compressed`` - *(bool)* - compress output CSVs instead of raw write
   (*default=True*, e.g. append ‘.gz’)
-  ``all_frames`` - *(bool)* - for video-based events, log all instances
   in box or just the center (*default=False*)
-  ``time_offset`` - *(int)* - when merging events for an asset split
   into multiple parts, time in seconds (*default=0*); negative numbers
   will cause a truncation (skip) of events happening before the zero
   time mark *(added v0.7.1)*
-  ``verbose`` - *(bool)* - verbose input/output configuration printing
   (*default=False*)
-  ``extractor`` - *(string)* - specify one extractor to flatten,
   skipping nested module import (*default=all*, e.g. ``dsai_metadata``)
-  ``generator`` - *(string)* - specify one generator for output,
   skipping nested module import (*default=all*, e.g. ``flattened_csv``)

generated schema
----------------

The output of this flattening will be a set of CSV files, one for each
extractor. the standard schema for these CSV files has the following
fields.

-  ``time_begin`` = time in seconds of event start
-  ``time_end`` = time in seconds of end (may be equal to time_start if
   instantaneous)
-  ``time_event`` = exact time in seconds (may be equal to time_start if
   instantaneous)
-  ``source_event`` = source media for event to add granularity for
   event inpact (e.g. face, video, audio, speech, image, ocr, script)
-  ``tag`` = simple text word or phrase
-  ``tag_type`` = descriptor for type of tag; e.g. tag=concept/label/emotion, keyword=special word,
   shot=segment, transcript=text, moderation=moderation, word=text/speech word,
   phrase=long utterance, face=face emotion/properties, identity=face or speaker
   recognition, scene=semantic scenes, brand=product or logo mention
-  ``score`` = confidence/probability
-  ``details`` = possible bounding box or other long-form (JSON-encoded)
   details
-  ``extractor`` = name of extractor for insight

dependencies
------------

| To install package dependencies in a fresh system, the recommended
  technique is a set of
| vanilla pip packages. The latest requirements should be validated from
  the ``requirements.txt`` file but at time of writing, they were the
  following.

.. code:: shell

   pip install --no-cache-dir -r requirements.txt 

Execution and Deployment
========================

This package is meant to be run as a one-off processing tool that
aggregates the insights of other extractors.

command-line standalone
-----------------------

Run the code as if it is an extractor. In this mode, configure a few
environment variables to let the code know where to look for content.

One can also run the command-line with a single argument as input and
optionally ad runtime configuration (see `runtime
variables <#getting-started>`__) as part of the ``EXTRACTOR_METADATA``
variable as JSON.

.. code:: shell

   EXTRACTOR_METADATA='{"compressed":true}'

Locally Run on Results
~~~~~~~~~~~~~~~~~~~~~~

For utility, the above line has been wrapped in the bash script
``run_local.sh``.

.. code:: shell

   EXTRACTOR_METADATA='$3' EXTRACTOR_NAME=metadata-flatten EXTRACTOR_JOB_ID=1 \
       EXTRACTOR_CONTENT_PATH=$1 EXTRACTOR_CONTENT_URL=file://$1 EXTRACTOR_RESULT_PATH=`pwd`/results/$2 \
       python -u main.py

This allows a simplified command-line specification of a run
configuration, which also allows the passage of metadata into a
configuration.

*Normal result generation into compressed CSVs (with overwrite).*

.. code:: shell

   ./run_local.sh data/wHaT3ver1t1s results/

*Result generation with environment variables and integration of results
from a file that was split at an offset of three hours.*

.. code:: shell

   ./run_local.sh results/1XMDAz9w8T1JFEKHRuNunQhRWL1/ results/ '{"force_overwrite":false,"time_offset":10800}'

*Result generation from a single extractor, with its nested directory
explicitly specified. (added v0.6.1)*

.. code:: shell

   ./run_local.sh results/dsai_metadata results/ '{"extractor":"dsai_metadata"}'

Local Runs with Timing Offsets
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The script ``run_local.sh`` also searches for a text file called
``timing.txt`` in each source directory. If found, it will offset all
results by the specified number of seconds before saving them to disk.
Also, negative numbers will cause a truncation (skip) of events
happening before the zero time mark. *(added v0.7.1)*

This capability may be useful if you have to manually split a file into
multiple smaller files at a pre-determined time offset (e.g. three hours
-> 10800 in ``timing.txt``). *(added v0.5.2)*

.. code:: shell

   echo "10800" > 1XMDAz9w8T1JFEKHRuNunQhRWL1/timing.txt
   ./run_local.sh results/1XMDAz9w8T1JFEKHRuNunQhRWL1/ results/

Afterwards, new results can be added arbitrarily and the script can be
rerun in the same directory to accomodate different timing offsets.

*Example demonstrating integration of multiple output directories.*

.. code:: shell

   find results -type d  -d 1 | xargs -I {} ./run_local.sh {} results/

ContentAI
---------

Deployment
~~~~~~~~~~

Deployment is easy and follows standard ContentAI steps.

.. code:: shell

   contentai deploy --cpu 256 --memory 512 metadata-flatten
   Deploying...
   writing workflow.dot
   done

Alternatively, you can pass an image name to reduce rebuilding a docker
instance.

.. code:: shell

   docker build -t metadata-deploy
   contentai deploy metadata-flatten --cpu 256 --memory 512 -i metadata-deploy

Locally Downloading Results
~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can locally download data from a specific job for this extractor to
directly analyze.

.. code:: shell

   contentai data wHaT3ver1t1s --dir data

Run as an Extractor
~~~~~~~~~~~~~~~~~~~

.. code:: shell

   contentai run https://bucket/video.mp4  -w 'digraph { aws_rekognition_video_celebs -> metadata_flatten}'

   JOB ID:     1Tfb1vPPqTQ0lVD1JDPUilB8QNr
   CONTENT:    s3://bucket/video.mp4
   STATE:      complete
   START:      Fri Feb 15 04:38:05 PM (6 minutes ago)
   UPDATED:    1 minute ago
   END:        Fri Feb 15 04:43:04 PM (1 minute ago)
   DURATION:   4 minutes 

   EXTRACTORS

   my_extractor

   TASK      STATE      START           DURATION
   724a493   complete   5 minutes ago   1 minute 

Or run it via the docker image…

::

   docker run --rm  -v `pwd`/:/x -e EXTRACTOR_CONTENT_PATH=/x/file.mp3 -e EXTRACTOR_RESULT_PATH=/x/result2 <docker_image>

View Extractor Logs (stdout)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: shell

   contentai logs -f <my_extractor>
   my_extractor Fri Nov 15 04:39:22 PM writing some data
   Job complete in 4m58.265737799s

Testing
=======

Testing is included via tox.  To launch testing for the entire package, just run `tox` at the command line. 
Testing can also be run for a specific file within the package by setting the evironment variable `TOX_ARGS`.

.. code:: shell

   TOX_ARG=test_basic.py tox 
   


Future Development
==================

-  the remaining known extractors...  ``openpose``, ``dsai_tmstext_classifier_extractor``, 
    ``dsai_vinyl_sound_ai``, ``dsai_name_entity_extractor``, ``gcp_videointelligence_text_detection``,
    ``aws_rekognition_video_segments``
-  integration of viewership insights
-  creation of sentiment and mood-based insights (which tags most
   co-occur here?)
