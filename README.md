# heanbian-elasticsearch

## 依赖条件

JDK 21

Elasticsearch 8.14.1

## pom.xml

具体版本，可以从 Maven Central 获得

```
<dependency>
  <groupId>com.heanbian</groupId>
  <artifactId>heanbian-elasticsearch</artifactId>
  <version>20.2</version>
</dependency>
```

## 使用示例

```
String connectionString = "elasticsearch://<username>:<password>@<ip1>:<port1>,<ip2>:<port2>";
ElasticsearchTemplate esTemplate = new ElasticsearchTemplate(connectionString);
```

说明：暂无。