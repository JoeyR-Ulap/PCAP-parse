FROM python:alpine3.7
COPY . /app
WORKDIR /app
RUN pip3 install -r requirements.txt
EXPOSE 8080
ENTRYPOINT python3 main.py
