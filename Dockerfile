FROM haifengat/ctp_real_md
COPY *.py /home/
COPY *.yml /home/
COPY *.txt /home/
RUN pip install -r /home/requirements.txt
ENV pg_addr postgresql://postgres:123456@172.19.129.98:15432/postgres
ENTRYPOINT ["python", "/home/xml_pg.py"]
