FROM python:3.7-slim

ARG user=cae
ARG uid=2000
ARG gid=2000

ENV WORKDIR=/src
ENV VIDEO=/videos/video.mp4
ENV MANIFEST=/tmp/NO_MANIFEST_PROVIDED
ENV SYMLINK=metadata-static

# install pacakages
COPY requirements.txt $WORKDIR/requirements.txt
COPY app/browse/requirements.txt $WORKDIR/app/browse/requirements.txt
COPY app/quality/requirements.txt $WORKDIR/app/quality/requirements.txt

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
    # install requirements (base package)
    && pip install --no-cache-dir -r $WORKDIR/requirements.txt \
    # install app requirements
    && pip install --no-cache-dir -r $WORKDIR/app/browse/requirements.txt \
    && pip install --no-cache-dir -r $WORKDIR/app/quality/requirements.txt \
    # install NLP word model for spacy
    && su $user && python -m spacy download en_core_web_sm \
    # convert to user permissions
    && chown -R $uid:$gid /$user/.streamlit

COPY . $WORKDIR/.

# stagger docker file creation for better cache formulation
RUN python -V \
    # copy and install local packages
    && pip install --no-cache-dir $WORKDIR \
    # add symlink (v 0.9.4)
    && python $WORKDIR/app/static_link_create.py --target /tmp/$SYMLINK --name $SYMLINK \
    # convert to user permissions, make temp writeable
    && chown -R $uid:$gid $WORKDIR /tmp/$SYMLINK \
    && chmod a+wx /tmp/$SYMLINK

# exposing default port for streamlit
EXPOSE 8501 8601

# launch as a specific user
USER $user

# run apps
CMD python -V && \
    echo "cd $WORKDIR/app/quality" > /tmp/run_script.sh  && \
    echo "nohup gunicorn -k gevent --workers=1 --bind=0.0.0.0:8601 -t 90  \"server:app(manifest='$MANIFEST', media_file='$VIDEO', data_dir='/results')\" & " >> /tmp/run_script.sh && \
    echo "cd $WORKDIR/app/browse" >>  /tmp/run_script.sh && \
    echo "nohup streamlit run --server.enableCORS false timed.py -- --manifest $MANIFEST --media_file $VIDEO --data_dir /results --symlink /tmp/$SYMLINK & " >> /tmp/run_script.sh && \
    echo "tail -f  $WORKDIR/app/browse/nohup.out $WORKDIR/app/quality/nohup.out " >> /tmp/run_script.sh && \
    chmod +x /tmp/run_script.sh && \
    /tmp/run_script.sh
