FROM python
WORKDIR /app
COPY requirements.txt requirements.txt
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
COPY . /app
EXPOSE 9009
EXPOSE 8095
