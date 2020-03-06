# example script for local run
if [ $# -lt 1 ]; then
	echo "./run_local.sh <result_json_source> <result_output_sub> [<json_args>] - run flattening for existing director (downloaded from a single job)"
	echo "  e.g. ./run_local.sh results/SOMESUBID results/ \"{'force_overwrite':False}\" -- will re-run flatteners "
	echo "  e.g. find results -type d -d 1 | xargs -I {} ./run_local.sh {} results/ -- will run all flatteners in sub-dir"
    echo "" 
    echo " NOTE: This script also searches for a text file called 'timing.txt' in each source directory.  If found, it will "
    echo "       offset all results by the specified number of seconds before saving them to disk. "
    exit -1
fi

RUNARGS="$3"
if [ -f "$1/timing.txt" ]; then
    OFFSET=$(cat "$1/timing.txt")
    echo "Detected timing file '$1/timing.txt' with offset $OFFSET seconds..."
    RUNARGS="{\"force_overwrite\":false, \"time_offset\":$OFFSET}"
    echo "Overwriting extractor metadata to : '$RUNARGS'"
fi

EXTRACTOR_METADATA="$RUNARGS" EXTRACTOR_NAME=dsai_metadata-flatten EXTRACTOR_JOB_ID=1 \
    EXTRACTOR_CONTENT_PATH=$1 EXTRACTOR_CONTENT_URL=file://$1 EXTRACTOR_RESULT_PATH=$2 \
    python -u metadata_flatten/main.py
