
FROM quay.io/keboola/docker-custom-python:latest
COPY keboola_component_template /code/
WORKDIR /data/
RUN pip install -r /code/requirements.txt
CMD ["python", "-u", "/code/src/main.py"]