FROM public.ecr.aws/lambda/python:3.10

# install LibreOffice
RUN yum -y install curl wget tar gzip zlib freetype-devel \
    libxslt \
    libxslt1-dev \
    gcc \
    ghostscript \
    lcms2-devel \
    libffi-devel \
    libimagequant-devel \
    libjpeg-devel \
    libraqm-devel \
    libtiff-devel \
    libwebp-devel \
    make \
    openjpeg2-devel \
    rh-python36 \
    rh-python36-python-virtualenv \
    sudo \
    tcl-devel \
    tk-devel \
    tkinter \
    which \
    xorg-x11-server-Xvfb \
    zlib-devel \
    java \
    && yum clean all
RUN wget http://download.documentfoundation.org/libreoffice/stable/24.2.6/rpm/x86_64/LibreOffice_24.2.6_Linux_x86-64_rpm.tar.gz
RUN tar -xvzf LibreOffice_24.2.6_Linux_x86-64_rpm.tar.gz
RUN cd LibreOffice_24.2.6.2_Linux_x86-64_rpm/RPMS; yum -y localinstall *.rpm;
RUN yum -y install cairo
RUN cd ${LAMBDA_TASK_ROOT}

# Chromium dependencies
RUN yum install -y -q unzip
RUN yum install -y https://dl.google.com/linux/chrome/rpm/stable/x86_64/google-chrome-stable-126.0.6478.126-1.x86_64.rpm
# RUN yum install -y https://dl.google.com/linux/direct/google-chrome-stable_current_x86_64.rpm
# Install Chromium
COPY install-browser.sh /tmp/
RUN /usr/bin/bash /tmp/install-browser.sh

COPY . ${LAMBDA_TASK_ROOT}
RUN pip install --upgrade pip -q
RUN pip install -r requirements.txt

CMD ["main_daily.handler"]