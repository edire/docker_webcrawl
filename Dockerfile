FROM python:slim

#Install Slim Dependencies
RUN apt-get update && apt-get install -y curl gnupg git

#Chromium
RUN apt-get update && apt-get install -y chromium-driver

#ODBC
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
	curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN exit
RUN apt-get update && \
	ACCEPT_EULA=Y apt-get install -y msodbcsql17

# Install Python dependencies.
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

RUN mkdir /app
COPY ./app /app
WORKDIR /app

CMD ["python", "app.py"]