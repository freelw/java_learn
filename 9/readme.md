1. 初始化命令

      mvn archetype:generate                               \
            -DarchetypeGroupId=org.apache.flink              \
            -DarchetypeArtifactId=flink-quickstart-java      \
            -DarchetypeVersion=1.10.0 \
            -DgroupId=wangli_flink_to_mysql -DartifactId=flink_to_mysql_test \
            -DinteractiveMode=false

2. 找不到flink包的处理方法

      在pom.xml中去掉 flink依赖的scope

3. 找不到slf4j包的处理方法

      在exclude中去掉相关的配置

4. Could not resolve substitution to a value: ${akka.stream.materializer} 的处理方法

      pom里添加
      <transformer implementation="org.apache.maven.plugins.shade.resource.AppendingTransformer">
            <resource>reference.conf</resource>
      </transformer>

