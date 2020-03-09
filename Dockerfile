FROM python:3.7-slim
MAINTAINER  Eric Zavesky <ezavesky@research.att.com>

ARG WORKDIR=/usr/src/app
ARG PYPI_INSTALL=""
ENV TMPAPP /tmp/app

# install pacakages
WORKDIR $WORKDIR
COPY . $TMPAPP

RUN python -V \
    # create user ID and run mode
    # && groupadd -g $gid $user && useradd -m -u $uid -g $gid $user \
    # && apt-get update \
    # && apt-get -y install git vim \
    && cd $TMPAPP && pip install --no-cache-dir . \
    # clean up mess from other apt-actions
    && apt-get -qq -y remove \
    && apt-get -qq -y autoremove \
    && apt-get autoclean \
    # clean up temp app
    && cd $WORKDIR && rm -rf $TMPAPP


EXPOSE 9101
CMD metadata-flatten
