FROM apache/airflow:2.10.2
# Set environment variables to bypass SSL verification (not recommended for production)
# ENV PYTHONHTTPSVERIFY=0
# ENV CURLOPT_SSL_VERIFYHOST=0
# ENV CURLOPT_SSL_VERIFYPEER=0
# Copy the requirements.txt file into the container
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
# Switch to root user to install additional packages
# USER root
# 
# # Update package list and install ca-certificates
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     ca-certificates \
#     apt-transport-https \
#     gnupg
#
# # Update CA certificates
# RUN update-ca-certificates
# # Switch back to airflow user
# USER airflow

# Your additional commands here