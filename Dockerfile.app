FROM python:3.7-slim

ARG user=cae
ARG uid=2000
ARG gid=2000
ARG spacy_model=en_core_web_md
# larger model for bigger memory docker systems
# ARG spacy_model=en_core_web_lg

ENV WORKDIR=/src
ENV VIDEO=/videos/video.mp4
ENV MANIFEST=/tmp/NO_MANIFEST_PROVIDED
ENV SYMLINK=metadata-static

# install pacakages
COPY requirements.txt $WORKDIR/requirements.txt
COPY app/browse/requirements.txt $WORKDIR/app/browse/requirements.txt
COPY app/quality/requirements.txt $WORKDIR/app/quality/requirements.txt
COPY app/lexicon_map/requirements.txt $WORKDIR/app/lexicon_map/requirements.txt

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
enableXsrfProtection = false\n\
" > /$user/.streamlit/config.toml' \
    # install requirements (base package)
    && pip install --no-cache-dir -r $WORKDIR/requirements.txt \
    # --- browser app
    && pip install --no-cache-dir -r $WORKDIR/app/browse/requirements.txt \
    # --- quality app
    # && pip install --no-cache-dir -r $WORKDIR/app/quality/requirements.txt \
    # --- lexicon-map app
    && pip install --no-cache-dir -r $WORKDIR/app/lexicon_map/requirements.txt \
    # install NLP word model for gensim - https://github.com/RaRe-Technologies/gensim-data (350M, 1G, 1.6G below)
    #  python -m gensim.downloader --download glove-wiki-gigaword-300 \
    #  python -m gensim.downloader --download word2vec-google-news-300 \
    #  python -m gensim.downloader --download fasttext-wiki-news-subwords-300 \
    # install NLP word model for spacy (used by both browser and lexicon-map)
    && python -m spacy download $spacy_model \
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

RUN touch /tmp/run_script.sh && \
    echo "touch \$WORKDIR/app/browse/nohup.out \$WORKDIR/app/lexicon_map/nohup.out " >> /tmp/run_script.sh && \
    # --- quality app
    # echo "cd \$WORKDIR/app/quality" >> /tmp/run_script.sh  && \
    # echo "nohup gunicorn -k gevent --workers=1 --bind=0.0.0.0:8601 --timeout 120  \"server:app(manifest='\$MANIFEST', media_file='\$VIDEO', data_dir='/results')\" & " >> /tmp/run_script.sh && \
    # --- mapping app
    echo "cd \$WORKDIR/app/lexicon_map" >> /tmp/run_script.sh  && \
    # (no gunicorn?!) echo "nohup gunicorn -k gevent --workers=1 --threads=1 --bind=0.0.0.0:8701 --timeout 240  \"server:app(data_dir='/results', mapping_model='$spacy_model', model_target='default')\" & " >> /tmp/run_script.sh && \
    echo "nohup python server.py --data_dir /results --manifest '\$MANIFEST' --mapping_model '$spacy_model' --model_target='default' -p 8701 & " >> /tmp/run_script.sh && \
    # --- browse app
    echo "cd $WORKDIR/app/browse" >>  /tmp/run_script.sh && \
    echo "nohup streamlit run --server.enableCORS false timed.py -- --manifest \$MANIFEST --media_file \$VIDEO --mapping_moodel $spacy_model --data_dir /results --symlink /tmp/\$SYMLINK & " >> /tmp/run_script.sh && \
    # --- talk what's happening
    echo "sleep 2 " >> /tmp/run_script.sh && \
    
    echo "tail -f  \$WORKDIR/app/browse/nohup.out \$WORKDIR/app/lexicon_map/nohup.out " >> /tmp/run_script.sh && \
    chmod +x /tmp/run_script.sh && \
    cp /tmp/run_script.sh $WORKDIR && \
    chown -R $uid:$gid $WORKDIR


# exposing default port for streamlit
EXPOSE 8501 8601 8701

# launch as a specific user
USER $user

# run apps
CMD python -V && \
    $WORKDIR/run_script.sh 

