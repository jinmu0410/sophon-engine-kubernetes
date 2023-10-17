package com.jm.sophon.engine.kubernetes.spark.deployment.model;

import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.util.HashMap;
import java.util.Map;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/8/4 10:10
 */
@Data
@SuperBuilder
public class SparkConfig {

    private String yamlContent;
    private String sparkHome;
    private String master = "";
    private String mainClass = "";
    private String[] args;
    private String k8sConfigFile;

    private String k8sCarCertData;
    private String k8sClientCrtData;
    private String k8sClientKeyData;

    private String k8sServiceAccount;

    private String k8sMasterUrl;

    private String k8sUsername;

    private String k8sPassword;

    private String k8sImage;

    private String k8sImagePullPolicy;

    private String k8sLanguageType;

    private String k8sRestartPolicy;

    private String k8sSparkVersion;

    private String k8sNamespace;
    private String k8sFileUploadPath;
    private String deployMode = "client"; // ("client") // todo cluster
    private String appResource; // ("mainApplicationFile")
    private String appName; // ("")
    private String jars; // ("--jars", "")
    private String packages; // ("--packages", "")
    private String excludePackages; // ("--exclude-packages", "")
    private String repositories; // ("--repositories", "")
    private String files; // ("--files", "")
    private String archives; // ("--archives", "")
    private Map<String, String> conf = new HashMap<>(); // ("", "")
    private String propertiesFile; // ("")
    private String driverMemory; // ("--driver-memory", "")
    private String driverJavaOptions; // ("--driver-java-options", "")
    private String driverLibraryPath; // ("--driver-library-path", "")
    private String driverClassPath; // ("--driver-class-path", "")
    private String executorMemory; // ("--executor-memory", "")
    private String proxyUser; // ("--proxy-user", "")
    private boolean verbose = false; // (false)
    private Integer driverCores; // ("--driver-cores", "") // Cluster deploy mode only
    private String totalExecutorCores; // ("--total-executor-cores", "")
    private Integer executorCores; // ("--executor-cores", "")
    private Integer numExecutors; // ("--num-executors", "")
    private String principal; // ("--principal", "")
    private String keytab; // ("--keytab", "")
    private String queue; // ("--queue", "")
}
