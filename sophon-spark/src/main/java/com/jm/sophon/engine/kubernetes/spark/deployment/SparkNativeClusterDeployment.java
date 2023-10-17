package com.jm.sophon.engine.kubernetes.spark.deployment;

import com.jm.sophon.engine.kubernetes.spark.deployment.core.AbstractNativeClusterDeployment;
import com.jm.sophon.engine.kubernetes.spark.deployment.core.SophonContext;

import java.util.List;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/10/9 19:59
 */
public class SparkNativeClusterDeployment extends AbstractNativeClusterDeployment {

    private static final String SPARK_HOME = "SPARK_HOME";


    public SparkNativeClusterDeployment(SophonContext sophonContext) {
        super(sophonContext);
    }

    @Override
    protected List<String> buildCommand() {
        return null;
    }

    @Override
    public void pre() {
        //todo 如果spark启动的服务器不在k8s集群上，需要把k8s集群~/.kube/config文件复制一份到当前主机同样目录下
        if(!checkKubeConfigFileIsExits()){
            LOG.error("kubernetes config not exits");
        }



    }

    private Boolean checkKubeConfigFileIsExits() {

        return true;
    }


    @Override
    public void post() {

    }

    @Override
    public void cancel() {

    }
}
