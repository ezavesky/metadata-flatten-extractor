FROM python:3.7-slim

RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg

# streamlit-specific commands
RUN mkdir -p /root/.streamlit
RUN bash -c 'echo -e "\
[general]\n\
email = \"\"\n\
" > /root/.streamlit/credentials.toml'
RUN bash -c 'echo -e "\
[server]\n\
enableCORS = false\n\
" > /root/.streamlit/config.toml'

# exposing default port for streamlit
EXPOSE 8501

# copy over and install packages
COPY app/requirements.txt ./requirements.txt
RUN pip3 install -r requirements.txt

# copying all analysis code to image
COPY _version.py /

# run app
CMD streamlit run /app/timed.py
