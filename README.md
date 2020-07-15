# tick_xml_pg

#### 介绍
xml数据包由期货公司提供,本程序将xml数据包存入pg tick数据表中


#### 使用说明
环境变量
> xml_zip_path
  xml压缩包路径
> pg_conn
  postgres连接字串 postgres://user:password@ip:port/db

## Dockerfile
```dockerfile
FROM haifengat/pyctp:2.3.2
COPY *.py /home/
RUN pip install -r /home/requirements.txt
ENV pg_addr postgresql://postgres:123456@172.19.129.98:15432/postgres
ENTRYPOINT ["python", "/home/xml_pg.py"]
```

### build
```bash
# 通过github git push触发 hub.docker自动build 到标签latest
# 执行下面语句生成 yyyyMMdd的标签
docker pull haifengat/xml_tick_pg && docker tag haifengat/xml_tick_pg haifengat/xml_tick_pg:`date +%Y%m%d` && docker push haifengat/xml_tick_pg:`date +%Y%m%d`
```

### RUN
```bash
docker run -itd --name xml_pg --privileged -e xml_zip_path="/xml" -e pg_addr="postgresql://user:pwd@pg_server_ip:5432/postgres" -v ${宿主xml文件路径}:/xml/ haifengat/xml_tick_pg
# 查看日志
docker logs -f xml_pg
```