FROM haifengat/centos:8.1
COPY *.py /home/
RUN pip install -r /home/requirements.txt
ENV pg_addr postgresql://postgres:123456@172.19.129.98:25432/postgres
ENTRYPOINT ["python", "/home/xml_pg.py"]
