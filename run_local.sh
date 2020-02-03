# example script for local run
if [ $# -eq 0 ]; then
	echo "./run_local.sh <result_directory> - run flattening for existing director (downloaded from a single job)"
    exit -1
fi
EXTRACTOR_NAME=metadata-flatten EXTRACTOR_JOB_ID=1 EXTRACTOR_CONTENT_PATH=$1 EXTRACTOR_CONTENT_URL=file://$1 EXTRACTOR_RESULT_PATH=`pwd`/results/$2 python -u main.py
