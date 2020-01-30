FROM python:3.7-slim
MAINTAINER  Eric Zavesky <ezavesky@research.att.com>

ARG WORKDIR=/usr/src/app
ARG PYPI_INSTALL=""
# ARG PYPI_INSTALL=" --index-url http://dockercentral.it.att.com:8093/nexus/repository/pypi-group/simple --trusted-host dockercentral.it.att.com"

# install pacakages
WORKDIR $WORKDIR
COPY . $WORKDIR

RUN python -V \
    # create user ID and run mode
    # && groupadd -g $gid $user && useradd -m -u $uid -g $gid $user \
    # && apt-get update \
    # && apt-get -y install git vim \
    && pip install $PYPI_INSTALL --no-cache-dir -r $WORKDIR/requirements.txt \
    # clean up mess from other apt-actions
    && apt-get -qq -y remove \
    && apt-get -qq -y autoremove \
    && apt-get autoclean 


EXPOSE 9101
CMD  python -u ./main.py
