package com.jm.sophon.engine.kubernetes.spark.deployment.model;

import cn.hutool.core.map.MapUtil;
import lombok.Data;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/10/19 14:30
 */

@Data
@SuperBuilder
public class SparkShellModel {
    public String sparkHome;
    public String master;
    public String deployMode;
    public String name;
    public String mainClass;
    public String mainJarPath;
    public String[] arguments;
    public String jars;
    public String packages;
    public String files;
    public String archives;
    public Map<String,String> confMap;
    public SparkExecutorModel sparkExecutorModel;
    public SparkDriverModel sparkDriverModel;
    public SparkKubernetesModel sparkKubernetesModel;


    //static
    public static String joint = " \n";
    public static String SPARK_MASTER = "--master ";
    public static String SPARK_DEPLOY_MODE = "--deploy-mode ";
    public static String SPARK_NAME = "--name ";
    public static String SPARK_MAIN_CLASS = "--class ";
    public static String SPARK_JARS = "--jars ";
    public static String SPARK_PACKAGES = "--packages ";
    public static String SPARK_FILES = "--files ";
    public static String SPARK_ARCHIVES = "--archives ";
    public static String SPARK_CONF = "--conf ";
    public static String SPARK_DRIVER_CORES = "--driver-cores ";
    public static String SPARK_DRIVER_MEMORY = "--driver-memory ";
    public static String SPARK_DRIVER_JAVA_OPTIONS = "--driver-java-options ";
    public static String SPARK_DRIVER_CLASS_PATH = "--driver-class-path ";
    public static String SPARK_DRIVER_LIBRARY_PATH = "--driver-library-path ";
    public static String SPARK_EXECUTOR_MEMORY = "--executor-memory ";
    public static String SPARK_EXECUTOR_CORES = "--executor-cores ";
    public static String SPARK_NUM_EXECUTORS = "--num-executors ";
    public static String SPARK_TOTAL_EXECUTOR_CORES = "--total-executor-cores ";
    public static String SPARK_K8S_NAMESPACE = "spark.kubernetes.namespace";
    public static String SPARK_K8S_DRIVER_SC = "spark.kubernetes.authenticate.driver.serviceAccountName";
    public static String SPARK_K8S_EXECUTOR_SC = "spark.kubernetes.authenticate.executor.serviceAccountName";
    public static String SPARK_K8S_IMAGE = "spark.kubernetes.container.image";
    public static String SPARK_K8S_IMAGE_PULLPOLICY = "spark.kubernetes.container.image.pullPolicy";




    public static String SPARK_EQUALS = "=";


    //构建shell命令
    public String buildShell(){
        StringBuilder builder = new StringBuilder(sparkHome + joint);

        //必须的
        builder.append(SPARK_MASTER + master + joint);
        builder.append(SPARK_DEPLOY_MODE + deployMode + joint);
        builder.append(SPARK_NAME + name + joint);
        builder.append(SPARK_MAIN_CLASS + mainClass + joint);

        //非必须的
        if(StringUtils.isNotBlank(jars)){
            builder.append(SPARK_JARS + jars + joint);
        }
        if(StringUtils.isNotBlank(packages)){
            builder.append(SPARK_PACKAGES + packages + joint);
        }
        if(StringUtils.isNotBlank(files)){
            builder.append(SPARK_FILES + files + joint);
        }
        if(StringUtils.isNotBlank(archives)){
            builder.append(SPARK_ARCHIVES + archives + joint);
        }
        //driver
        if(sparkDriverModel != null){
            if(StringUtils.isNotBlank(sparkDriverModel.getDriverCores())){
                builder.append(SPARK_DRIVER_CORES + sparkDriverModel.getDriverCores() + joint);
            }
            if(StringUtils.isNotBlank(sparkDriverModel.getDriverMemory())){
                builder.append(SPARK_DRIVER_MEMORY + sparkDriverModel.getDriverMemory() + joint);
            }
            if(StringUtils.isNotBlank(sparkDriverModel.getDriverClassPath())){
                builder.append(SPARK_DRIVER_CLASS_PATH + sparkDriverModel.getDriverClassPath() + joint);
            }
            if(StringUtils.isNotBlank(sparkDriverModel.getDriverJavaOptions())){
                builder.append(SPARK_DRIVER_JAVA_OPTIONS + sparkDriverModel.getDriverJavaOptions() + joint);
            }
            if(StringUtils.isNotBlank(sparkDriverModel.getDriverLibraryPath())){
                builder.append(SPARK_DRIVER_LIBRARY_PATH + sparkDriverModel.getDriverLibraryPath() + joint);
            }
        }
        //executor
        if(sparkExecutorModel != null){
            if(StringUtils.isNotBlank(sparkExecutorModel.getExecutorCores())){
                builder.append(SPARK_EXECUTOR_CORES + sparkExecutorModel.getExecutorCores() + joint);
            }
            if(StringUtils.isNotBlank(sparkExecutorModel.getExecutorMemory())){
                builder.append(SPARK_EXECUTOR_MEMORY + sparkExecutorModel.getExecutorMemory() + joint);
            }
            if(StringUtils.isNotBlank(sparkExecutorModel.getNumExecutors())){
                builder.append(SPARK_NUM_EXECUTORS + sparkExecutorModel.getNumExecutors() + joint);
            }
            if(StringUtils.isNotBlank(sparkExecutorModel.getTotalExecutorCores())){
                builder.append(SPARK_TOTAL_EXECUTOR_CORES + sparkExecutorModel.getTotalExecutorCores() + joint);
            }
        }
        //k8s base
        if(sparkKubernetesModel != null){
            if(StringUtils.isNotBlank(sparkKubernetesModel.getK8sNamespace())){
                builder.append(SPARK_CONF + SPARK_K8S_NAMESPACE + SPARK_EQUALS + sparkKubernetesModel.getK8sNamespace() + joint);
            }
            if(StringUtils.isNotBlank(sparkKubernetesModel.getK8sServiceAccount())){
                builder.append(SPARK_CONF + SPARK_K8S_DRIVER_SC + SPARK_EQUALS + sparkKubernetesModel.getK8sServiceAccount() + joint);
                builder.append(SPARK_CONF + SPARK_K8S_EXECUTOR_SC + SPARK_EQUALS + sparkKubernetesModel.getK8sServiceAccount() + joint);
            }
            if(StringUtils.isNotBlank(sparkKubernetesModel.getK8sImage())){
                builder.append(SPARK_CONF + SPARK_K8S_IMAGE + SPARK_EQUALS + sparkKubernetesModel.getK8sImage() + joint);
            }
            if(StringUtils.isNotBlank(sparkKubernetesModel.getK8sImagePullPolicy())){
                builder.append(SPARK_CONF + SPARK_K8S_IMAGE_PULLPOLICY + SPARK_EQUALS + sparkKubernetesModel.getK8sImagePullPolicy() + joint);
            }
        }

        //默认
        if(MapUtil.isNotEmpty(confMap)){
            confMap.entrySet().forEach(e->{
                builder.append(SPARK_CONF + e.getKey() + SPARK_EQUALS + e.getValue() + joint);
            });
        }

        builder.append(mainJarPath);
        if(arguments != null && arguments.length >0){
            builder.append(joint);
            builder.append(StringUtils.join(arguments," "));
        }

        return builder.toString();
    }


    @Data
    @SuperBuilder
    public static class SparkExecutorModel{
        public String executorMemory; // ("--executor-memory", "")
        public String executorCores; // ("--executor-cores", "")
        public String numExecutors; // ("--num-executors", "")
        public String totalExecutorCores; // ("--total-executor-cores", "")
    }

    @Data
    @SuperBuilder
    public static class SparkDriverModel{
        private String driverMemory; // ("--driver-memory", "")
        private String driverJavaOptions; // ("--driver-java-options", "")
        private String driverLibraryPath; // ("--driver-library-path", "")
        private String driverClassPath; // ("--driver-class-path", "")
        private String driverCores; // ("--driver-cores", "") // Cluster deploy mode only
    }

    @Data
    @SuperBuilder
    public static class SparkKubernetesModel{
        private String k8sConfigFile;
//        private String k8sCarCertData;
//        private String k8sClientCrtData;
//        private String k8sClientKeyData;
        private String k8sServiceAccount;
        private String k8sMasterUrl;
//        private String k8sUsername;
//        private String k8sPassword;
        private String k8sImage;
        private String k8sImagePullPolicy;
        private String k8sLanguageType;
        private String k8sRestartPolicy;
        private String k8sSparkVersion;
        private String k8sNamespace;
//        private String k8sFileUploadPath;
    }

}
