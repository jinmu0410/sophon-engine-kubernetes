package com.jm.sophon.engine.kubernetes;

import com.jm.sophon.engine.kubernetes.spark.config.KubernetesClientAdapter;
import com.jm.sophon.engine.kubernetes.spark.operator.RestartPolicy;
import com.jm.sophon.engine.kubernetes.spark.operator.SparkApplication;
import com.jm.sophon.engine.kubernetes.spark.operator.SparkApplicationSpec;
import com.jm.sophon.engine.kubernetes.spark.operator.SparkPodSpec;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.WritableOperation;

import java.util.HashMap;
import java.util.Map;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/10/3 15:15
 */
public class SparkApplicationTest {

    public static void main(String[] args) {

        String nginx = "apiVersion: apps/v1\n" +
                "kind: Deployment\n" +
                "metadata:\n" +
                "  name: demo-tomcat\n" +
                "  namespace: spark-operator\n" +
                "  labels:\n" +
                "    app: demo\n" +
                "spec:\n" +
                "  replicas: 1\n" +
                "  selector:\n" +
                "    matchLabels:\n" +
                "      app: demo\n" +
                "  template:\n" +
                "    metadata:\n" +
                "      labels:\n" +
                "        app: demo\n" +
                "    spec:\n" +
                "      containers:\n" +
                "      - name: tomcat\n" +
                "        image: demo-tomcat:121.0 \n" +
                "        ports:\n" +
                "        - containerPort: 80";

        KubernetesClientAdapter kubernetesClientAdapter = new KubernetesClientAdapter();
        KubernetesClient client = kubernetesClientAdapter.getClient();

        WritableOperation<HasMetadata> operation = client.resource(nginx).dryRun();



    }


    public static void sparkOperatorTest(){
        SparkApplication sparkApplication = new SparkApplication();
        ObjectMeta metadata = new ObjectMeta();
        metadata.setName("test-spark-operator");
        metadata.setNamespace("spark-operator");
        sparkApplication.setMetadata(metadata);

        SparkPodSpec driver =
                SparkPodSpec.Builder()
                        .cores(1)
                        .memory("1G")
                        .serviceAccount("my-release-spark")
                        .build();

        SparkPodSpec executor =
                SparkPodSpec.Builder()
                        .cores(1)
                        .instances(1)
                        .memory("1G")
                        .build();

        Map<String, String> sparkConfMap = new HashMap<>();

        SparkApplicationSpec sparkApplicationSpec =
                SparkApplicationSpec.Builder()
                        .type("Scala")
                        .mode("cluster")
                        .image("apache/spark:3.3.1")
                        .imagePullPolicy("Always")
                        .mainClass("org.apache.spark.examples.SparkPi")
                        .mainApplicationFile("local:///opt/spark/examples/jars/spark-examples_2.12-3.3.1.jar")
                        .sparkVersion("3.3.1")
                        .restartPolicy(new RestartPolicy("Never"))
                        .driver(driver)
                        .executor(executor)
                        .sparkConf(sparkConfMap)
                        .build();

        sparkApplication.setSpec(sparkApplicationSpec);

        KubernetesClientAdapter kubernetesClientAdapter = new KubernetesClientAdapter();
        KubernetesClient client = kubernetesClientAdapter.getClient();

        SparkApplication application = client.resource(sparkApplication).create();
    }
}
