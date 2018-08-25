#  Licensed to the Apache Software Foundation (ASF) under one   *
#  or more contributor license agreements.  See the NOTICE file *
#  distributed with this work for additional information        *
#  regarding copyright ownership.  The ASF licenses this file   *
#  to you under the Apache License, Version 2.0 (the            *
#  "License"); you may not use this file except in compliance   *
#  with the License.  You may obtain a copy of the License at   *
#                                                               *
#    http://www.apache.org/licenses/LICENSE-2.0                 *
#                                                               *
#  Unless required by applicable law or agreed to in writing,   *
#  software distributed under the License is distributed on an  *
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY       *
#  KIND, either express or implied.  See the License for the    *
#  specific language governing permissions and limitations      *
#  under the License.                                           *

FROM python:3.6-stretch

ARG AIRFLOW_VERSION
ARG FORCE_GPL
ENV VERSION=${AIRFLOW_VERSION:-1.10.0}
ENV DEBIAN_FRONTEND=noninteractive
ENV AIRFLOW_GPL_UNIDECODE=${FORCE_GPL:-true}

# install deps
RUN apt-get update -y && apt-get install -y \
        wget \
        libczmq-dev \
        curl \
        libssl-dev \
        inetutils-telnet \
        bind9utils \
        zip \
        unzip \
    && apt-get clean \
    && apt-get purge -y python2.7 \
    && apt-get -y --purge autoremove

RUN pip install --upgrade pip

COPY requirements.txt ./

# Since we install vanilla Airflow, we also want to have support for Postgres and Kubernetes
RUN pip install -r requirements.txt

# install airflow
ADD https://github.com/apache/incubator-airflow/archive/${VERSION}.zip /tmp/
RUN cd /tmp \
    && unzip ${VERSION}.zip \
    && cd incubator-airflow-${VERSION} \
    && rm -rf airflow/example_dags \
    && python setup.py install

COPY airflow-env-init.sh /tmp/airflow-env-init.sh

COPY bootstrap.sh /bootstrap.sh
RUN chmod +x /bootstrap.sh

ENTRYPOINT ["/bootstrap.sh"]
