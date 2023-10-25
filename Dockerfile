# Use the official Apache Airflow image as a base image
FROM apache/airflow:2.7.2

# Copy your requirements file into the image
ADD requirements.txt .

# Install the specified version of Apache Airflow and any additional dependencies
RUN pip install -r requirements.txt