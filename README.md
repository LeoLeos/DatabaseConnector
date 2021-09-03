# 连接数据库api包DatabaseConnector  
## 版本   
V1.0  
## 目标  
简化连接数据库代码冗余  
## 项目架构  
```
├─core
│   ├─DatabaseInfoSettings
│   │   ├─db.yaml   # 存放生产环境的数据库账号密码
│   │   └─db-dev.yaml   # 存放测试环境的数据库账号密码
│   ├─core.py
│   │   └─class Connector(object)
│   └─__init__.py
├─__init__.py
└─Read.md
```
## 使用方法  
实例化连接服务器:
```
connector=Connector(server_name, remote)
```
获取该服务器的所有数据库:
```
connector.show_database()
```
查找数据库名称:
```
connector.show_database("name")
```
查询数据:
```
connector.query("sql语句")
```
保存到数据库
```
connector.save_to_database("数据库名", "表名")
```
# DatabaseConnector
