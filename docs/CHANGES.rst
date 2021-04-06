Changes
=======

A method to flatten generated JSON data into timed CSV events in support
of analytic workflows within the `ContentAI Platform <https://www.contentai.io>`__.

1.4
---

1.4.0
~~~~~
- fix for timing offsets; don't overwrite any output if timing offset indicator

1.4.1
~~~~~
- add new `dsai_ads_detector` parser for predictive ad locations


1.3
---

1.3.3
~~~~~
- minor fix for `azure_videoindexer` parsing, now first video shot can *not* contain a keyframe ? 


1.3.2
~~~~~
- minor fix for `gcp_videointelligence_text_detection` parsing

1.3.1
~~~~~
- fix for no-output generators
- fix complete output for returned dictionary of data
- add richer documentation for library/api usage

1.3.0
~~~~~
- update output of main parse function to return a dict instead of file listing
- modify generator specification to allow ALL (``*`` **default**) or NONE for outputs


1.2
---

1.2.2
~~~~~
- add parsers for `gcp_videointelligence_text_detection`, `comskip_json`, `ibm_max_audio_classifier`, 
   `gcp_videointelligence_object_tracking`, `gcp_videointelligence_people_detection`
- improve testing to iterate over known set of data in testing dir
- fix generator/parser retrieve for whole name matches, not partials
- add documentation for new types, explicitly call out `person` tag_type
- update the `dsai_activity_emotions` parser to return tag type `emotion` (matching that of other AWS, Azure parsers)

1.2.1
~~~~~
- update `azure_videoindexer` for `tag_type` in detected brands (was speech, now video)

1.2.0
~~~~~
- add unit-testing to package build
- add command-line / parser input as complement to contentai-driven ENV variables
- fix bugs around specification of result path or specific generator

1.1
---

1.1.8
~~~~~
- fix issue about constant reference
- fix `run_local.sh` script for extra run param config
- fix querying for local files in non-contentai environments (regression since 1.1.0)

1.1.7
~~~~~
- inclusion of other constants for compatibility with other packages
- refactor/rename of parser classes to mandate a filename output prefix (e.g. ``flatten_``)
- add ``dsai_activity_emotions`` parser (a clone of ``dsai_activity_classifier``)

1.1.6
~~~~~
- remove applications, fork to new `metatata-database` source, to be posted
  at a `pypi database package <https://pypi.org/project/contentai-metadata-database>`__

1.1.4
~~~~~
- name update for ``dsai_moderation_image`` extractor

1.1.3
~~~~~
- hotfix for build distribution
- fix for content creation in streamlit/browsing app

1.1.2
~~~~~
- deployed extractor (docker fix) for updated namespace


1.1.1
~~~~~
- docs update, testing fixes, version bump for publication

1.1.0
~~~~~
- rename to ``contentai-metadata-flatten`` and publish to pypi as a package!


1.0
---

1.0.2
~~~~~
- update documentation for `Metadata Browser <app_browser>`__ and `Inventory Discovery <app_inventory>`__ app

1.0.1
~~~~~
- add ability to parse input CSVs but not segment into shot
- move to a single NLP library (spacy) for applications, using large model (with vectors)

1.0.0
~~~~~
- add new `dash/plotly <https://dash.plotly.com/>`__ driven quality check application

0.9
---

0.9.9
~~~~~
- update to optimize the pull of asset keys

0.9.7
~~~~~

- upgrade to use new `contentai extractor package <https://pypi.org/project/contentaiextractor/>`__
- update parser logic for safer key and data retrieval


0.9.6
~~~~~

- upgrade to use new `contentai extractor package <https://pypi.org/project/contentaiextractor/>`__
- update parser logic for safer key and data retrieval


0.9.6
~~~~~
- small tweaks/normalization of rounding factor for extractors
- correct emotion souce type for azure
- refactor app location for primary streamlit browser
  - fix mode discovery for modules with specific UX interface
- update file listing to show data bundle files as well
- refactor utilities script for reuse in other apps


0.9.5
~~~~~

- update to parse new version of `dsai_places`
- add new parser for `detectron2` extractor

0.9.4
~~~~~

- add static file serving to streamlit app, inspired by this `streamlit issue discussion <https://github.com/streamlit/streamlit/issues/400>`_
- modify some pages to point to downloadable tables (with button click)
- create new download page/mode that lists the generated and source files
- minor refactor of app's docker image for better caching in local creation and testing


0.9.3
~~~~~

- add ``dsai_moderation_text`` parser, update ``dsai_moderation`` parser for version robustness
  - add min threshold (*0.05*) to both moderation detectors


0.9.2
~~~~~

- add recursion to file-based discovery method for processed assets
  - unify read of JSON and text files with internalaized function call in extractor base class
- fix some extractors to use single name reference ``self.EXTRACTOR``

0.9.1
~~~~~

- fix transcript parsing in ``azure_videoindexer`` component
- add speaker differentiation as an identity block in ``azure_videoindexer`` (similar to ``aws_transcribe``)


0.9.0
~~~~~

- add timeline viewing to the ``event_table`` mode of streamlit app



0.8
---

0.8.9
~~~~~

- fixes to main streamlit app for partial extractors (e.g. missing identity, sparse brand)

0.8.8
~~~~~

- add parser for ``dsai_moderation``


0.8.7
~~~~~

- add parser for ``dsai_activity_classifier``
- fix bug for faulty rejection of ``flatten_aws_transcribe`` results

0.8.6
~~~~~

- add parsers for ``pyscenedetect``, ``dsai_sceneboundary``, ``aws_transcribe``, ``yolo3``, ``aws_rekognition_video_text_detect``
- add speaker identity (from speech) to ``gcp_videointelligence_speech_transcription``
- add ``type`` field (maps to ``tag_type``) to output generated by ``wbTimeTaggedTmetadata`` generator
  - add hashing against data (e.g. ``box``) within JSON metadata generator


0.8.5
~~~~~

- add parsers for ``dsai_yt8m`` (youtube8M or mediapipe)


0.8.4
~~~~~

- add parsers for ``dsai_activity_slowfast`` (activity) and ``dsai_places`` (scene/settings)
- add *source_type* sub-field to ``event_table`` browsing mode


0.8.3
~~~~~

- add ``manifest`` option to application for multiple assets
- fix app docker file for placement/generation of code with a specific user ID
- fix CI/CD integration for auto launch
- fix app explorer bugs (derive 'words' from transcript/keywords if none)


0.8.2
~~~~~

- hotfix for missing data in ``dsai_metadata`` parser


0.8.2
~~~~~

- slight refactor of how parsers are discovered, to allow search by name or type (for use as package)
- fix package import for contentai local file
- switch *tag_type* of ``ocr`` to ``transcript`` and ``ocr`` for *source_type* (``azure_videoindexer``)


0.8.1
~~~~~

- adding music parser ``dsai_musicnn`` for different audio regions


0.8.0
~~~~~

- convert to package for other modules to install
- switch document to RST from MD
- add primitive testing capabilities (to be filled)


0.7
---

0.7.1
~~~~~

-  added truncation/trim of events before zero mark if time offset is
   negative
-  re-brand extractor as ``dsai_metadata_flatten`` for ownership
   consistency

0.7.0
~~~~~

-  create new set of generator class objects for varying output
   generator
-  add new ``generator`` input for limiting output to a single type


0.6
---

0.6.2
~~~~~

-  rename ``rekognition_face_collection`` to
   ``aws_rekognition_face_collection`` for consistency


0.6.1
~~~~~

-  split documentation and changes
-  add new ``cae_metadata`` type of parser
-  modify ``source_type`` of detected faces in ``azure_videoindexer`` to
   ``face``
-  modify to add new ``extractor`` input for limit to scanning (skips
   sub-dir check)

0.6.0
~~~~~

-  adding CI/CD script for `gitlab <https://gitlab.com>`__
-  validate usage as a flattening service
-  modify ``source_type`` for ``aws_rekognition_video_celebs`` to
   ``face``

0.5
---


0.5.4
~~~~~

-  adding ``face_attributes`` visualization mode for exploration of face
   data
-  fix face processing to split out to ``tag_type`` as ``face`` with
   richer subtags

0.5.3
~~~~~

-  add labeling component to application (for video/image inspection)
-  fix shot duration computeation in application (do not overwrite
   original event duration)
-  add text-search for scanning named entities, words from transcript


0.5.2
~~~~~

-  fix bugs in ``gcp_videointelligence_logo_recognition`` (timing) and
   ``aws_rekognition_video_faces`` (face emotions)
-  add new detection of ``timing.txt`` for integration of multiple
   results and their potential time offsets
-  added ``verbose`` flag to input of main parser
-  rename ``rekognition_face_collection`` for consistency with other
   parsers


0.5.1
~~~~~

-  split app modules into different visualization modes (``overview``,
   ``event_table``, ``brand_expansion``)

   -  ``brand_expansion`` uses kNN search to expand from shots with
      brands to similar shots and returns those brands
   -  ``event_table`` allows specific exploration of identity
      (e.g. celebrities) and brands witih image/video playback
   -  **NOTE** The new application requires ``scikit-learn`` to perform
      live indexing of features

-  dramatically improved frame targeting (time offset) for event
   instances (video) in application


0.5.0
~~~~~

-  split main function into sepearate auto-discovered modules
-  add new user collection detection parser
   ``rekognition_face_collection`` (custom face collections)


0.4
---


0.4.5
~~~~~

-  fixes for gcp moderation flattening
-  fixes for app rendering (switch most graphs to scatter plot)
-  make all charts interactive again
-  fix for time zone/browser challenge in rendering


0.4.4
~~~~~

-  fixes for ``azure_videoindexer`` parser
-  add sentiment and emotion summary
-  rework graph generation and add bran/entity search capability


0.4.3
~~~~~

-  add new ``azure_videoindexer`` parser
-  switch flattened reference from ``logo`` to ``brand``; ``explicit``
   to ``moderation``
-  add parsing library ``pytimeparse`` for simpler ingest
-  fix bug to delete old data bundle if reference files are available


0.4.2
~~~~~

-  add new ``time_offset`` parameter to environment/run configuration
-  fix bug for reusing/rewriting existing files
-  add output prefix ``flatten_`` to all generated CSVs to avoid
   collision with other extractor input


0.4.1
~~~~~

-  fix docker image for nlp tasks, fix stop word aggregation


0.4.0
~~~~~

-  adding video playback (and image preview) via inline command-line
   execution of ffmpeg in application
-  create new Dockerfile.app for all-in-one explorer app creation


0.3
---


0.3.2
~~~~~

-  argument input capabilities for exploration app
-  sort histograms in exploration app by count not alphabet


0.3.1
~~~~~

-  browsing bugfixes for exploration application


0.3.0
~~~~~

-  added new `streamlit <https://www.streamlit.io/>`__ code for `data
   explorer interface <app>`__

   -  be sure to install extra packages if using this app and starting
      from scratch (e.g. new flattened files)
   -  if you’re working from a cached model, you can also drop it in
      from a friend


0.2
---


0.2.1
~~~~~

-  schema change for verb/action consistency ``time_start`` ->
   ``time_begin``
-  add additional row field ``tag_type`` to describe type of tag (see
   `generated-insights <#generated-insights>`__)
-  add processing type ``gcp_videointelligence_logo_recognition``
-  allow compression as a requirement/input for generated files
   (``compressed`` as input)

0.2.0
~~~~~

-  add initial package, requirements, docker image
-  add basic readme for usage example
-  processes types ``gcp_videointelligence_label``,
   ``gcp_videointelligence_shot_change``,
   ``gcp_videointelligence_explicit_content``,
   ``gcp_videointelligence_speech_transcription``,
   ``aws_rekognition_video_content_moderation``,
   ``aws_rekognition_video_celebs``, ``aws_rekognition_video_labels``,
   ``aws_rekognition_video_faces``,
   ``aws_rekognition_video_person_tracking``,

