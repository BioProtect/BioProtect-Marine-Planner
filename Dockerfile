FROM ubuntu:20.04 as server
RUN apt-get update && \
    apt-get install -y software-properties-common \
    && apt-get clean \
    && apt-get autoremove \
    && rm -rf /var/lib/apt/lists/*

# RUN add-apt-repository ppa:ubuntugis/ppa  && \ 
#     apt-get update 

RUN apt-get update && apt-get install -y build-essential \
    python3 \
    gdal-data \ 
    libgdal26 \
    python3-gdal \ 
    gdal-bin \ 
    python3-dev\
    python3-pip \
    git-core \
    libpq-dev \
    && apt-get clean \
    && apt-get autoremove \
    && rm -rf /var/lib/apt/lists/*

RUN export CPLUS_INCLUDE_PATH=/usr/include/gdal \
    && export C_INCLUDE_PATH=/usr/include/gdal
RUN pip3 install gdal 
#==$(gdal-config --version)

COPY requirements.txt requirements.txt
COPY server.dat server.dat
COPY . .

RUN pip3 install -r requirements.txt


# RUN ln -sf /proc/self/fd/1 /var/log/nginx/access.log && \
#     ln -sf /proc/self/fd/1 /var/log/nginx/error.log
ADD https://github.com/ufoscout/docker-compose-wait/releases/download/2.7.3/wait /wait
RUN chmod +x /wait && chmod a+x marxan-server.py

EXPOSE 80

CMD /wait && python3 marxan-server.py
