# Use an official Python base image
FROM python:3.11.7-slim

# Set the working directory inside the container
WORKDIR /app
RUN mkdir -p /app/logs

# Install dependencies
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk-headless \
    curl \
    procps \
    cron \
    && apt-get clean

# Set environment variables for Java
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Copy the project files into the container
COPY . /app

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Add a cron job file
COPY cronjob /etc/cron.d/etl_cronjob

# Set permissions for the cron job file
RUN chmod 0644 /etc/cron.d/etl_cronjob

# Apply the cron job
RUN crontab /etc/cron.d/etl_cronjob

# Start the cron service and the script
CMD ["cron", "-f"]
