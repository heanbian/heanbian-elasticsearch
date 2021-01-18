# heanbian-elasticsearch

## 前提条件
JDK11+

## pom.xml

具体版本，可以上Maven中央仓库查询

```
<dependency>
	<groupId>com.heanbian.block</groupId>
	<artifactId>heanbian-elasticsearch</artifactId>
	<version>1.0.0</version>
</dependency>
```

## 使用示例

```
String connectionString = "elasticsearch://<username>:<password>@<ip1>:<port1>,<ip2>:<port2>";
ElasticsearchTemplate esTemplate = new ElasticsearchTemplate(connectionString);
```

说明：暂无。