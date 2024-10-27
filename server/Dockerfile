FROM python:alpine
WORKDIR /app

ADD main.py /app/main.py
ADD requirements.txt /app/requirements.txt
RUN pip install -r requirements.txt

ENTRYPOINT ["fastapi","dev","main.py"]
