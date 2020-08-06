Ad Inventory Discovery and Forecasting
======================================

An interactive application has been created for inventory exploration
in a proof-of-concept fashion.

- Lexicon Mapping (for inventory)

    ... is an interactive `Dash <https://dash.plotly.com/>`__ app for
    exploration and mapping new query terms to events flattened with this package starting
    from `flattened metadata <README.md>`__.

    1. `Getting Started <#getting-started-lexicon-mapper>`__
    2. `Browser Execution <#lexicon-mapper-execution-and-deployment>`__


Getting Started Lexicon Mapper
==============================

This web app runs in python via a light-weight wrapper and the dash
package. It has the capability to show these event insights
out-of-the-box.

- mapping between input words and a target lexicon
- discovery/generation of a target lexicon from detected ContentAI output
- filtering and plotting of the hits for a target campaign using detected tags
- one-click review of events surronding an inventory moment in time for a specific asset
- mapping between input creatives and a target lexicon for contextual placement (future)
- coarse-level forecasting of hits based on temporal example matching (future)

The application can be configured with these options.

.. code:: shell 

    usage: server.py [-h] [-p PORT] [-z RESULT_COUNT] [-l LOG_SIZE]
                    [-r REFRESH_INTERVAL] [--verbose] [--data_dir DATA_DIR]
                    [--model_target MODEL_TARGET] [-n MAPPING_MODEL]

    Lexicon Mapper

    optional arguments:
    -h, --help            show this help message and exit
    -p PORT, --port PORT  Port for HTTP server (default: 8701)
    -z RESULT_COUNT, --result_count RESULT_COUNT
                            Max results per page
    -r REFRESH_INTERVAL, --refresh_interval REFRESH_INTERVAL
                            Refresh interval for log (in millis)
    --verbose, -v

    model configuration:
    --data_dir DATA_DIR   specify the source directory for model
    --model_target MODEL_TARGET
                            name of the target model to validate/generate
    -n MAPPING_MODEL, --mapping_model MAPPING_MODEL
                            spacy mapping model if NLP models installed -
                            https://spacy.io/models


A command-line example of this functionality.  Assuming that thre are flattened results
in the directory `data`, the below commands can bootstrap the interface.

.. code:: shell

    # uses the 'data' directory for new model storage, and will create a new dataset called 'default'
    python server.py  --data_dir data/ --model_target default

    # just load the standard NLP embedding model for 'mapped results'
    python server.py 


Installation
------------

To install dependencies, just use the requirements file. This will install
dash and a few processing librarieis in your environment.  

.. code:: shell

   pip install -r app/lexicon_map/requirements.txt

The application does use `spaCy <https://spacy.io/>`__ for some basic
NLP tasks, so after the above installation, run the command below to get
the right pre-built model.

.. code:: shell

   python -m spacy download en_core_web_lg  (this vocab is larger but it provides the required vocabulary file)



Docker installation & execution
-------------------------------

An application-oriented docker file has also been created. It makes the
following assumptions for operation…

.. code:: shell

   # Build docker image from root directory of repo
   docker build -t lexicon -f Dockerfile.app .

-  Assumes extracted dataset is in “/results” 

   -  *NOTE: You must mount both of these directories in the docker run
      command.*

-  Connect to your exposed application via port 8701

   -  From your localhost, go to ``localhost:8701``
   -  If connecting externally, use the IP addresses listed on the
      console

Afterwards, running your docker file is trivial with standard syntax to
mount the target volumes where the directory `data` is expected to have
output from a flattening process.

.. code:: shell

   # Run docker container (default video path)
   docker run -it --rm -p 8701:8701 -v ${PWD}/data:/results lexicon

Optionally you can edit the app while running for continuous updates.

.. code:: shell

   # Mounting app rather than copying it allows you to edit the app while container is running
   docker run --rm -p 8701:8701 -v ${PWD}/data:/results -v ${PWD}/app:/src/app lexicon:latest


Data Ingest
-----------

For each `model_target` specified above, a discovery and encoding process will occur.
Depending on the length of your asset (almost linearly) and the count of 
etractors, the code will proceed to load all flattened files, convert time signatures, and
perform some basic NLP tasks.

(timing TBD)
*On a 2.9 GHz 8-core laptop, this process took about 7 minutes for 16 assets 
with average of 3 event input files. (v1.0.2)*

Once complete, a cached ingested data file will be created and stored as
`described above <#Execution-and-Deployment>`__.



Lexicon Mapper Execution and Deployment
=======================================

More information to be provided.
