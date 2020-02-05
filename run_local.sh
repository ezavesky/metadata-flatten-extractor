# example script for local run
if [ $# -lt 1 ]; then
	echo "./run_local.sh <result_json_source> <result_output_sub> [<json_args>] - run flattening for existing director (downloaded from a single job)"
	echo "  e.g. ./run_local.sh results/SOMESUBID results/. \"{'force_overwrite':False}\" "
    exit -1
fi
EXTRACTOR_METADATA="$3" EXTRACTOR_NAME=metadata-flatten EXTRACTOR_JOB_ID=1 \
    EXTRACTOR_CONTENT_PATH=$1 EXTRACTOR_CONTENT_URL=file://$1 EXTRACTOR_RESULT_PATH=$2 \
    python -u main.py
