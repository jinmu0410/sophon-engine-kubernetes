package com.jm.sophon.engine.kubernetes;

import com.jm.sophon.engine.kubernetes.spark.deployment.model.SparkShellModel;

import java.util.HashMap;
import java.util.Map;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/10/19 14:58
 */
public class SparkShellModelTest {

    public static void main(String[] args) {

        Map<String,String> confMap = new HashMap<>();
        confMap.put("spark.executor.instances","5");
        confMap.put("spark.kubernetes.namespace","apache-spark");
        confMap.put("spark.kubernetes.authenticate.driver.serviceAccountName","spark-service-account");
        confMap.put("spark.kubernetes.authenticate.executor.serviceAccountName","spark-service-account");
        confMap.put("spark.kubernetes.container.image","apache/spark:3.3.1");
        confMap.put("spark.kubernetes.container.image.pullPolicy","IfNotPresent");


        SparkShellModel sparkShellModel = SparkShellModel.builder()
                .sparkHome("/Users/jinmu/Downloads/soft/spark-3.3.1-bin-hadoop3/bin/spark-submit")
                .deployMode("cluster")
                .master("k8s://https://lb.kubesphere.local:6443")
                .name("spark-pi")
                .mainClass("org.apache.spark.examples.SparkPi")
                .confMap(confMap)
                .mainJarPath("local:///opt/spark/examples/jars/spark-examples_2.12-3.3.1.jar")
                .arguments(new String[]{})
                .build();

        String shell = sparkShellModel.buildShell();

        System.out.println(shell);


    }
}
