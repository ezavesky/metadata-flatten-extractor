FROM python:3.7-slim

ENV WORKDIR=/src
ENV VIDEO=/videos/video.mp4

# install pacakages
COPY . $WORKDIR

RUN python -V \
    && apt-get update && apt-get install -y --no-install-recommends ffmpeg \
    # streamlit-specific commands
    && mkdir -p /root/.streamlit \
    && bash -c 'echo -e "\
[general]\n\
email = \"\"\n\
" > /root/.streamlit/credentials.toml'
RUN bash -c 'echo -e "\
[browser]\n\
gatherUsageStats = false\n\
[server]\n\
enableCORS = false\n\
" > /root/.streamlit/config.toml' \
    # install requirements
    && pip install --no-cache-dir  -r $WORKDIR/requirements.txt \ 
    && pip install --no-cache-dir  -r $WORKDIR/app/requirements.txt \
    # install NLP word model for spacy
    && python -m spacy download en_core_web_sm

# exposing default port for streamlit
EXPOSE 8501

# run app
CMD cd $WORKDIR/app && streamlit run timed.py -- --media_file $VIDEO --data_dir /results
