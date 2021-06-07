FROM python:3
WORKDIR /
RUN pip install --no-cache-dir flask requests aerospike
copy . .
EXPOSE 80
CMD ["python", "./webserver.py"] 
