# Use an official Python runtime as a parent image. Using slim-bookworm.
FROM python:3.10-slim-bookworm

# NEW: Run security updates and clean up in a single layer to reduce vulnerabilities
RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get autoremove -y \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

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

# NEW: Copy and make the entrypoint script executable
COPY entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/entrypoint.sh

# Expose the port that your application listens on.
EXPOSE 5000

# Define the command to run your startup script.
CMD ["/usr/local/bin/entrypoint.sh"]
