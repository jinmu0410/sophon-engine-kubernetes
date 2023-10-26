package com.jm.sophon.engine.kubernetes;

import com.jm.sophon.engine.kubernetes.spark.deployment.SparkNativeClusterDeployment;
import com.jm.sophon.engine.kubernetes.spark.deployment.SparkSophonContext;
import com.jm.sophon.engine.kubernetes.spark.deployment.model.SparkConfig;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/10/26 09:50
 */
public class SparkNativeTest {

    public static void main(String[] args) {

        start();

    }

    static void start(){
        SparkNativeClusterDeployment sparkNativeClusterDeployment = new SparkNativeClusterDeployment(buildContext());

        sparkNativeClusterDeployment.submit();
    }



    public static SparkSophonContext buildContext(){
        SparkSophonContext sparkSophonContext = new SparkSophonContext();
        SparkConfig sparkConfig = SparkConfig.builder()
                .sparkHome("/Users/jinmu/Downloads/soft/spark-3.3.1-bin-hadoop3/bin/spark-submit")
                .k8sMasterUrl("k8s://https://lb.kubesphere.local:6443")
                .k8sImage("apache/spark:3.3.1")
                .k8sImagePullPolicy("IfNotPresent")
                .k8sNamespace("apache-spark")
                .k8sServiceAccount("spark-service-account")
                .deployMode("cluster")
                .appName("spark-pi-test-num")
                .executorCores("1")
                .executorMemory("512m")
                .numExecutors("2")
                .driverCores("1")
                .driverMemory("512m")
                .mainClass("org.apache.spark.examples.SparkPi")
                .mainJar("local:///opt/spark/examples/jars/spark-examples_2.12-3.3.1.jar")
                .build();

        sparkSophonContext.setSparkConfig(sparkConfig);

        return sparkSophonContext;
    }
}
