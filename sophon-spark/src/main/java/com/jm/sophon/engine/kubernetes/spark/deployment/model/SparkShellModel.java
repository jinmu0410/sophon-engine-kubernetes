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

    // --conf spark.driver.cores=1
    public Map<String,String> confMap;

    // --conf spark.kubernetes.namespace=apache-spark
    public Map<String,String> kubernetesConfMap;

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

        //默认
        if(MapUtil.isNotEmpty(confMap)){
            confMap.entrySet().forEach(e->{
                builder.append(SPARK_CONF + e.getKey() + SPARK_EQUALS + e.getValue() + joint);
            });
        }

        //k8s
        if(MapUtil.isNotEmpty(kubernetesConfMap)){
            kubernetesConfMap.entrySet().forEach(e->{
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


}
