# Use an official Python runtime as a parent image.  We'll use slim-buster to keep it small.
FROM python:3.9-slim-buster

# Set the working directory in the container to /app.  This is where your code will live.
WORKDIR /app

# Copy the requirements file into the container.  This is a separate step so Docker can cache it.
COPY requirements.txt /app/requirements.txt

# Install the Python dependencies.  Use --no-cache-dir to keep the image size down.
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy the rest of your application code into the container.  This should be done *after* installing dependencies.
COPY . /app

# Expose the port that your application listens on.  If your app doesn't listen on a port, you can remove this.
# IMPORTANT:  Change 5000 to the actual port your Flask app uses, if it's different.
EXPOSE 5000

# Define the command to run your application.  This is what starts your app when the container runs.
CMD ["python", "app.py"]
