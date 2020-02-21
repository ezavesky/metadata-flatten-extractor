# metadata-flatten-extractor

A method to flatten generated JSON data into timed CSV events in support of analytic 
workflows within the [ContentAI Platform](https://www.contentai.io). For interactive
exploration a [data explorer interface](app) was created as a quick starting place for a quick start.

1. [Getting Started](#getting-started)
2. [Execution](#execution-and-deployment)
3. [Testing](#testing)
4. [Changes](#changes)
4. [Future Development](#future-development)

# Getting Started

This library is used as a [single-run executable](#contentai-standalone).  
Runtime parameters can be passed
for processing that configure the returned results and can be examined in more detail 
in the [main](main.py) script.  

**NOTE: Not all flattening functions will respect/obey properties defined here.**

* `force_overwrite` - *(bool)* - force existing files to be overwritten (*default=False*)
* `compressed` - *(bool)* - compress output CSVs instead of raw write (*default=True*, e.g. append '.gz')
* `all_frames` - *(bool)* - for video-based events, log all instances in box or just the center (*default=False*)
* `time_offset` - *(int)* - when merging events for an asset split into multiple parts, time in seconds (*default=0*)
* `verbose` - *(bool)* - verbose input/output configuration printing (*default=False*)


## generated schema

The output of this flattening will be a set of CSV files, one for each extractor.  the standard
schema for these CSV files has the following fields.

* `time_begin` = time in seconds of event start
* `time_end` = time in seconds of end (may be equal to time_start if instantaneous)
* `time_event` = exact time in seconds (may be equal to time_start if instantaneous)
* `source_event` =  source media for event to add granularity for event inpact (e.g. face, video, audio, speech, image)
* `tag` = simple text word or phrase
* `tag_type` = descriptor for type of tag; e.g. tag=concept/label, shot=segment, moderation=moderation, word=text/speech word, phrase=long utterance, face=face emotion/properties, identity=face recognition, person=person objects
* `score` = score/probability
* `details` = possible bounding box or other long-form (JSON-encoded) details
* `extractor` = name of extractor (from below)

## dependencies

To install package dependencies in a fresh system, the recommended technique is a set of  
vanilla pip packages.  The latest requirements should be validated from the `requirements.txt` file
but at time of writing, they were the following.

```shell 
pip install --no-cache-dir -r requirements.txt 
```

# Execution and Deployment
This package is meant to be run as a one-off processing tool that aggregates the insights of other extractors.

## command-line standalone

Run the code as if it is an extractor.  In this mode, configure a few environment variables
to let the code know where to look for content.

One can also run the command-line with a single argument as input and optionally ad runtime
configuration (see [runtime variables](#getting-started)) as part 
of the `EXTRACTOR_METADATA` variable as JSON.   

```shell
EXTRACTOR_METADATA='{"compressed":True}'
```

### locally downloading results

You can locally download data from a specific job for this extractor to directly analyze.

```shell
contentai data wHaT3ver1t1s --dir data
```

### locally run on results

For utility, the above line has been wrapped in the bash script `run_local.sh`.

```shell
EXTRACTOR_METADATA='$3' EXTRACTOR_NAME=metadata-flatten EXTRACTOR_JOB_ID=1 \
    EXTRACTOR_CONTENT_PATH=$1 EXTRACTOR_CONTENT_URL=file://$1 EXTRACTOR_RESULT_PATH=`pwd`/results/$2 \
    python -u main.py
```

This allows a simplified command-line specification of a run configuration, which also allows the passage of metadata into a configuration.

*Normal result generation into compressed CSVs (with overwrite).*

```shell
./run_local.sh data/wHaT3ver1t1s results/
```

*Result generation with environment variables and integration of results from a file that was split at an offset of three hours.*
 
```shell
./run_local.sh results/1XMDAz9w8T1JFEKHRuNunQhRWL1/ results/ '{"force_overwrite":false,"time_offset":10800}'
```

### Local Runs with Timing Offsets

The script `run_local.sh` also searches for a text file called `timing.txt` in each source directory.  If found, it will offset all results by the specified number of seconds before saving them to disk.

This capability may be useful if you have to manually split a file into multiple smaller files at a pre-determined time offset (e.g. three hours -> 10800 in `timing.txt`).   *(added v0.5.2)*

```shell
echo "10800" > 1XMDAz9w8T1JFEKHRuNunQhRWL1/timing.txt
./run_local.sh results/1XMDAz9w8T1JFEKHRuNunQhRWL1/ results/
```

Afterwards, new results can be added arbitrarily and the script can be rerun in the same directory to accomodate different timing offsets.

*Example demonstrating integration of multiple output directories.*

```shell
find results -type d  -d 1 | xargs -I {} ./run_local.sh {} results/
```

## ContentAI 

### Deployment

Deployment is easy and follows standard ContentAI steps.

```shell
contentai deploy --cpu 256 --memory 512 metadata-flatten
Deploying...
writing workflow.dot
done
```

Alternatively, you can pass an image name to reduce rebuilding a docker instance.

```shell
docker build -t metadata-deploy
contentai deploy metadata-flatten --cpu 256 --memory 512 -i metadata-deploy
```

### Run as an Extractor


```shell
contentai run s3://bucket/video.mp4 -w 'digraph { <my_extractor> }' --watch --tasks

JOB ID:     1Tfb1vPPqTQ0lVD1JDPUilB8QNr
CONTENT:    s3://bucket/video.mp4
STATE:      complete
START:      Fri Nov 15 04:38:05 PM (6 minutes ago)
UPDATED:    1 minute ago
END:        Fri Nov 15 04:43:04 PM (1 minute ago)
DURATION:   4 minutes 

EXTRACTORS

my_extractor

TASK      STATE      START           DURATION
724a493   complete   5 minutes ago   1 minute 
```


Or run it via the docker image...
```
docker run --rm  -v `pwd`/:/x -e EXTRACTOR_CONTENT_PATH=/x/file.mp3 -e EXTRACTOR_RESULT_PATH=/x/result2 <docker_image>
```

### view extractor logs (stdout)

```shell
contentai logs -f <my_extractor>
my_extractor Fri Nov 15 04:39:22 PM writing some data
Job complete in 4m58.265737799s
```

# Testing

(testing and validation forthcoming)

# Changes

## 0.5

### 0.5.4
* adding `face_attributes` visualization mode for exploration of face data

### 0.5.3
* add labeling component to application (for video/image inspection) 
* fix shot duration computeation in application (do not overwrite original event duration)
* add text-search for scanning named entities, words from transcript

### 0.5.2
* fix bugs in `gcp_videointelligence_logo_recognition` (timing) and `aws_rekognition_video_faces` (face emotions)
* add new detection of `timing.txt` for integration of multiple results and their potential time offsets
* added `verbose` flag to input of main parser
* rename `rekognition_face_collection` for consistency with other parsers

### 0.5.1
* split app modules into different visualization modes (`overview`, `event_table`, `brand_expansion`)
  * `brand_expansion` uses kNN search to expand from shots with brands to similar shots and returns those brands
  * `event_table` allows specific exploration of identity (e.g. celebrities) and brands witih image/video playback
  * **NOTE** The new application requires `scikit-learn` to perform live indexing of features
* dramatically improved frame targeting (time offset) for event instances (video) in application

### 0.5.0
* split main function into sepearate auto-discovered modules
* add new user collection detection parser `rekognition_face_collection` (custom face collections)

## 0.4

### 0.4.5
* fixes for gcp moderation flattening
* fixes for app rendering (switch most graphs to scatter plot)
* make all charts interactive again
* fix for time zone/browser challenge in rendering

### 0.4.4
* fixes for `azure_videoindexer` parser
* add sentiment and emotion summary
* rework graph generation and add bran/entity search capability

### 0.4.3
* add new `azure_videoindexer` parser
* switch flattened reference from `logo` to `brand`; `explicit` to `moderation`
* add parsing library `pytimeparse` for simpler ingest
* fix bug to delete old data bundle if reference files are available

### 0.4.2
* add new `time_offset` parameter to environment/run configuration
* fix bug for reusing/rewriting existing files
* add output prefix `flatten_` to all generated CSVs to avoid collision with other extractor input

### 0.4.1
* fix docker image for nlp tasks, fix stop word aggregation

### 0.4.0
* adding video playback (and image preview) via inline command-line execution of ffmpeg in application
* create new Dockerfile.app for all-in-one explorer app creation

## 0.3

### 0.3.2
* argument input capabilities for exploration app
* sort histograms in exploration app by count not alphabet

### 0.3.1
* browsing bugfixes for exploration application

### 0.3.0
* added new [streamlit](https://www.streamlit.io/) code for [data explorer interface](app)
  * be sure to install extra packages if using this app and starting from scratch (e.g. new flattened files)
  * if you're working from a cached model, you can also drop it in from a friend


## 0.2

### 0.2.1
* schema change for verb/action consistency `time_start` -> `time_begin`
* add additional row field `tag_type` to describe type of tag (see [generated-insights](#generated-insights))
* add processing type `gcp_videointelligence_logo_recognition`
* allow compression as a requirement/input for generated files (`compressed` as input)


### 0.2.0
* add initial package, requirements, docker image
* add basic readme for usage example
* processes types `gcp_videointelligence_label`, `gcp_videointelligence_shot_change`, `gcp_videointelligence_explicit_content`, `gcp_videointelligence_speech_transcription`, `aws_rekognition_video_content_moderation`, `aws_rekognition_video_celebs`, `aws_rekognition_video_labels`, `aws_rekognition_video_faces`, `aws_rekognition_video_person_tracking`, 


# Future Development

* the remaining known extractors... `pyscenedetect`, `yolo3`, `openpose`
