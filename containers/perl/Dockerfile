FROM public.ecr.aws/ubuntu/ubuntu:20.04_stable

ARG VERSION=5.32

LABEL container.base.image="public.ecr.aws/ubuntu/ubuntu:latest"
LABEL software.name="perl"
LABEL software.version=${VERSION}
LABEL software.description="Perl programming language"
LABEL software.website="https://www.perl.org/"
LABEL software.documentation="https://www.perl.org/docs.html"
LABEL software.license=""
LABEL tags="Perl"

RUN apt update

RUN apt install -y perl gcc make libpq-dev gpg git wget unzip

RUN cpan -T DBIx DBI LWP DBD::Pg

RUN git clone https://github.com/biosql/biosql.git /scripts/

RUN wget -qO- "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" > "awscliv2.zip"

RUN unzip -qq awscliv2.zip

RUN ./aws/install

RUN rm -rf awscliv2.zip