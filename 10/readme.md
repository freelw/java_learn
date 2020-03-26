1. init

    mvn archetype:generate                               \
            -DarchetypeGroupId=org.apache.flink              \
            -DarchetypeArtifactId=flink-quickstart-java      \
            -DarchetypeVersion=1.10.0 \
            -DgroupId=wangli_parquet -DartifactId=parquet_test \
            -DinteractiveMode=false