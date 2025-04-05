# Use an official Python runtime as a parent image.  We'll use slim-buster to keep it small.
FROM python:3.10-slim-buster

# Set the working directory in the container to /app.  This is where your code will live.
WORKDIR /app

# Copy the requirements file into the container.  This is a separate step so Docker can cache it.
COPY requirements.txt /app/requirements.txt

# Install the Python dependencies.  Use --no-cache-dir to keep the image size down.
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy the application code into the container.
COPY . /app

# Make a directory for config files
RUN mkdir /config

# Expose the port that your application listens on.
EXPOSE 5000

# Define the command to run your application.  Now, we specify the config file as a command-line argument.
CMD ["python", "app.py", "--config", "/config/config.json"]
