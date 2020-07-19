FROM haifengat/ctp_real_md
COPY *.py /home/
COPY requirements.txt /home/
RUN pip install -r /home/requirements.txt
ENV pg_addr postgresql://postgres:123456@pg_tick:15432/postgres
ENTRYPOINT ["python", "/home/xml_pg.py"]
