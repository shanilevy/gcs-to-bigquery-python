FROM python:3.11.2-slim

RUN python3 -m venv /opt/venv

# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED True

# Copy local code to the container image.
ENV APP_HOME /app
WORKDIR $APP_HOME
COPY . ./

# Install production dependencies.
RUN . /opt/venv/bin/activate && pip install -r requirements.txt

# Run the web service on container startup. Here we use the gunicorn
# webserver, with one worker process and 8 threads.
# For environments with multiple CPU cores, increase the number of workers
# to be equal to the cores available.
# Timeout is set to 0 to disable the timeouts of the workers to allow Cloud Run to handle instance scaling.
CMD . /opt/venv/bin/activate && exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app

# [END run_helloworld_dockerfile]
# [END cloudrun_helloworld_dockerfile]
