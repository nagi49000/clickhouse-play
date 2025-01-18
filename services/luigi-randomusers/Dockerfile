FROM python:3.12
ARG norootuser=default

RUN pip install \
    luigi==3.6.0 \
    requests==2.32.3 \
    clickhouse-connect==0.8.14 &&\
    useradd -ms /bin/bash ${norootuser}
USER default

WORKDIR /home/${norootuser}
COPY --chown=${norootuser}:${norootuser} python/clickhouse-randomuser clickhouse-randomuser

WORKDIR /home/${norootuser}/clickhouse-randomuser
ENV N_RECORD=40
ENV SLEEP_SECS=30
ENV LUIGI_WORKDIR=luigi-output
CMD bash run_pipeline_on_loop.sh