FROM apache/spark:latest as jupyter-local

# Switch to root temporarily to handle package installations if needed
USER root
RUN ln -sf /usr/bin/python3 /usr/bin/python

ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:${PYTHONPATH}"

# Copy and install workspace dependencies
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Set the working application directory inside the container
WORKDIR /opt/spark-apps

# Expose Jupyter interface and Spark internal driver communication ports
EXPOSE 8888
EXPOSE 29200
EXPOSE 4040