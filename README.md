# metadata-flatten-extractor

A method to flatten generated JSON data into timed CSV events in support of analytic 
workflows within the [ContentAI Platform](https://www.contentai.io).

1. [Getting Started](#getting-started)
2. [Testing](#testing)
3. [Changes](#changes)

# Getting Started

This library is used as a [single-run executable](#contentai-standalone).  
Runtime parameters can be passed
for processing that configure the returned results and can be examined in more detail 
in the [main](main.py) script.  

**NOTE: Not all flattening functions will respect/obey properties defined here.**

(in progress)
* `force_overwrite` - *(bool)* - force existing files to be overwritten
* `threshold_value` - *(float)* - the top N results (by min threshold) for each model  (default=0.5)

To install package dependencies in a fresh system, the recommended technique is a set of  
vanilla pip packages.  The latest requirements should be validated from the `requirements.txt` file
but at time of writing, they were the following.

```shell 
pip install --no-cache-dir -r requirements.txt 
```

## command-line standalone

Run the code as if it is an extractor.  In this mode, configure a few environment variables
to let the code know where to look for content.

One can also run the command-line with a single argument as input and optionally ad runtime
configuration (see [runtime variables](#getting-started)) as part 
of the `EXTRACTOR_METADATA` variable as JSON.   For utility, the below line has been wrapped
in the bash script `run_local.sh`.

```shell
EXTRACTOR_NAME=metadata-flatten EXTRACTOR_JOB_ID=1 EXTRACTOR_CONTENT_PATH=$1 EXTRACTOR_CONTENT_URL=file://$1 EXTRACTOR_RESULT_PATH=`pwd`/results python main.py
```

You can locally download data from a specific job for this extractor to directly analyze.

```shell
contentai data wHaT3ver1t1s --dir data
./run_local.sh data/wHaT3ver1t1s
```


- patch input of files for better compliance
- add documentation (standard)


### ContentAI Deploy 

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

### ContentAI Run


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

## 0.2

### 0.2.0
* add initial package, requirements, docker image
* add basic readme for usage example
* processes types `gcp_videointelligence_label`, `gcp_videointelligence_shot_change`, `gcp_videointelligence_explicit_content`, `gcp_videointelligence_speech_transcription`, `aws_rekognition_video_content_moderation`, `aws_rekognition_video_celebs`, `aws_rekognition_video_labels`,


# Future Development

* allow compression as a requirement/input for generated files?
* the remaining known extractors... `aws_rekognition_video_faces`, `aws_rekognition_video_person_tracking`, `azure_videoindexer`, `pyscenedetect`, `yolo3`, `openpose`