
FROM python:3.7


COPY requirements.txt /marxan-server/requirements.txt
COPY .server.dat.default ./marxan-server/server.dat

RUN pip install -r requirements.txt
COPY . ./marxan-server

EXPOSE 80

ENTRYPOINT ["python3", "marxan-server.py"]
