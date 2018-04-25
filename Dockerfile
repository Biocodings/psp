FROM continuumio/miniconda
MAINTAINER Desiree Davison <cmap-soft@broadinstitute.org>
LABEL psp.pipeline.clue.io.version="0.0.1"
LABEL psp.pipeline.clue.io.vendor="Connectivity Map"
RUN mkdir -p /cmap/bin && \
mkdir -p /cmap/psp/broadinstitute_psp && \
mkdir -p ~/.aws && \
#cd /cmap/ && \
#git clone https://github.com/cmap/psp.git
conda create -y -n psp -c bioconda pandas=0.20.3 scipy=0.19.0 h5py=2.7.0 cmapPy=2.0.1 requests=2.18.4 argparse=1.4.0 boto3
COPY . /cmap/psp/
COPY credentials ~/.aws/
WORKDIR /cmap/bin
COPY dry.sh /cmap/bin/dry
RUN ["chmod", "+x", "/cmap/bin/dry"]
ENTRYPOINT ["/bin/bash","dry"]