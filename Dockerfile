#FROM python:3-alpine
#ADD app.py /
#COPY requirements.txt /
#RUN pip install -r requirements.txt
#ENTRYPOINT ["python3", "./app.py"]
FROM python:3.8-slim-buster

RUN python3 -m venv /opt/venv

# Install dependencies:
COPY requirements.txt .
RUN . /opt/venv/bin/activate && pip install -r requirements.txt

# Run the application:
COPY app.py .
ENV PORT 8080
#COPY credentials.json .
CMD . /opt/venv/bin/activate && exec python app.py