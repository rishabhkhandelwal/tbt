# Use an official Python runtime as a parent image
FROM python:3.9
 
# Set the working directory in the container
WORKDIR /app
 
 
# Copy the current directory contents into the container at /app
COPY . /app
 
ENTRYPOINT ["python", "t_by_t_data.py"]
#WORKDIR /logs