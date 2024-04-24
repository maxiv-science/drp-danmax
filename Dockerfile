FROM python:3

ARG CI_COMMIT_SHA=0000

WORKDIR /tmp

COPY requirements.txt /tmp/requirements.txt

RUN python -m pip --no-cache-dir install -r requirements.txt

COPY src /tmp/src
RUN echo ${CI_COMMIT_SHA} > /code_version

CMD ["dranspose"]
