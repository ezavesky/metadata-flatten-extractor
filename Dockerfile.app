FROM python:3.7-slim

ENV WORKDIR=/src
ENV VIDEO=/videos/video.mp4
ENV MANIFEST=/tmp/NO_MANIFEST_PROVIDED

# install pacakages
COPY . $WORKDIR

ARG user=cae
ARG uid=2000
ARG gid=2000

RUN python -V \
    && groupadd -g $gid $user && useradd -m -u $uid -g $gid $user \
    && apt-get update && apt-get install -y --no-install-recommends ffmpeg \
    # streamlit-specific commands
    && mkdir -p /$user/.streamlit \
    && bash -c 'echo -e "\
[general]\n\
email = \"\"\n\
" > /$user/.streamlit/credentials.toml'
RUN bash -c 'echo -e "\
[browser]\n\
gatherUsageStats = false\n\
[server]\n\
enableCORS = false\n\
" > /$user/.streamlit/config.toml' \
    # install requirements
    && pip install --no-cache-dir  -r $WORKDIR/app/requirements.txt \
    # install NLP word model for spacy
    && su $user && python -m spacy download en_core_web_sm \
    # convert to user permissions
    && chown -R $uid:$gid /$user/.streamlit \
    && chown -R $uid:$gid $WORKDIR

# exposing default port for streamlit
EXPOSE 8501

# launch as a specific user
USER $user

# run app
CMD cd $WORKDIR/app/app_browse && streamlit run --server.enableCORS false timed.py -- --manifest $MANIFEST --media_file $VIDEO --data_dir /results
