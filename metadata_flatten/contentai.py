import os
from urllib import request, parse
import json

# environment variables
# The job_id given to this dag by the ContentAI Platform
job_id = os.getenv("EXTRACTOR_JOB_ID", 'not_a_job')

# The location of the asset you wish to perform inference on
content_url = os.getenv("EXTRACTOR_CONTENT_URL", "video.mp4")

# when you download the content running urllib.request.urlopen(http://127.0.0.1/content/)
#  you can access the artifact at content_url
content_path = os.getenv("EXTRACTOR_CONTENT_PATH", "video.mp4")

# write results to this location which you want saved to the Data Lake
result_path = os.getenv("EXTRACTOR_RESULT_PATH", ".")

# get extractor name
extractor_name = os.getenv("EXTRACTOR_NAME", "musicnn")

# get metadata
metadata = {}
if "EXTRACTOR_METADATA" in os.environ:
    try:
        metadata = json.loads(os.environ["EXTRACTOR_METADATA"])
    except json.decoder.JSONDecodeError as e:
        print(f"Warning: EXTRACTOR_METADATA unusual; `{os.environ['EXTRACTOR_METADATA']}` -> parsed exception {e}")

# whether or not we're running in the platform
running_in_contentai = False
if "RUNNING_IN_CONTENTAI" in os.environ:
    running_in_contentai = (os.environ["RUNNING_IN_CONTENTAI"] == "true")


# download content to work with locally
def download_content():
    if running_in_contentai:
        request.urlopen('http://127.0.0.1/content/')


# get a list of keys for specified extractor
def get_extractor_result_keys(extractor_name):
    if not running_in_contentai:
        return {}
    url = request.urlopen(
        f'http://127.0.0.1/results/{extractor_name}')
    data = url.read()
    encoding = url.info().get_content_charset('utf-8')
    return json.loads(data.decode(encoding))


# get the contents of a particular key
def get_extractor_results(extractor_name, key, is_json=True):
    if not running_in_contentai:
        return {}
    url = request.urlopen(
        f'http://127.0.0.1/results/{extractor_name}/{key}')
    data = url.read()
    encoding = url.info().get_content_charset('utf-8')
    if not is_json:
        return encoding
    return json.loads(data.decode(encoding))


# save results immediately (vs at process exit)
def save_results():
    if running_in_contentai:
        data = parse.urlencode("")
        data = data.encode("utf-8")
        req = request.Request('http://127.0.0.1/results/', data)
        request.urlopen(req)
