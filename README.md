# heanbian-elasticsearch-client

### `pom.xml` 添加，如下：

```xml

<dependency>
	<groupId>com.heanbian</groupId>
	<artifactId>heanbian-elasticsearch-client</artifactId>
	<version>11.2.5</version>
</dependency>

```
注：JDK 11+ ，具体最新版本，可以到maven官网查找。

### `application.yml` 配置，样例：

```yaml

elasticsearch.cluster-nodes: elasticsearch://<username>:<password>@<ip>:<port>,<ip>:<port>...
  
```

### 在 Spring boot 2.x 项目启动类 XxxApplication 上加注解 `@EnableElasticsearch` 样例:

```java

@EnableElasticsearch
@SpringBootApplication
public class XxxApplication {

}

```

### 类中，使用样例：

```java

@Autowired
ElasticsearchTemplate esTemplate;

```

source 数据类必须实现接口 `ElasticsearchId`

```
<T extends ElasticsearchId> source
```