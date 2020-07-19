# tick_xml_pg

#### 介绍
xml数据包由期货公司提供,本程序将xml数据包存入pg tick数据表中


#### 使用说明
以haifengat/ctp_real_md为基础
环境变量
> xml_zip_path
  xml压缩包路径
> pg_conn
  postgres连接字串 postgres://user:password@ip:port/db

```bash
# 按示例生成docker-compose.yml
docker-compose up -d
```

## Dockerfile
```dockerfile
FROM haifengat/ctp_real_md
COPY *.py /home/
COPY requirements.txt /home/
RUN pip install -r /home/requirements.txt
ENV pg_addr postgresql://postgres:123456@pg_tick:15432/postgres
ENTRYPOINT ["python", "/home/xml_pg.py"]
```

### build
```bash
# 通过github git push触发 hub.docker自动build 到标签latest
# 执行下面语句生成 yyyyMMdd的标签
docker pull haifengat/xml_tick_pg && docker tag haifengat/xml_tick_pg haifengat/xml_tick_pg:`date +%Y%m%d` && docker push haifengat/xml_tick_pg:`date +%Y%m%d`
```

### docker-compose.yml
```yaml
version: "3.1"
services:
    xml_pg:
        image: haifengat/xml_tick_pg
        container_name: xml_pg
        restart: always
        environment:
            - TZ=Asia/Shanghai
            - xml_zip_path=/xml
            - pg_conn=postgres://postgres:123456@pg_tick:5432/postgres
        volumes:
            - /mnt/future_xml:/xml
        depends_on:
            - pg_tick

    pg_tick:
        image: postgres:12
        container_name: pg_tick
        restart: always
        environment:
            - TZ=Asia/Shanghai
            - POSTGRES_PASSWORD=123456
        ports:
            - "35432:5432"
        volumes:
            - ./pgdata:/var/lib/postgresql/data
```
