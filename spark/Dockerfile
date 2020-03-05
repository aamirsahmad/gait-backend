# FROM eecsyorku/eecs4415
FROM gettyimages/spark
COPY . /app
WORKDIR /app
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
RUN echo "export GPG_TTY=$(tty)" >> ~/.profile
