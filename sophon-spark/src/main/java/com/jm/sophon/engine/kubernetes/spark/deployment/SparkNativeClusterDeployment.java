package com.jm.sophon.engine.kubernetes.spark.deployment;

import com.jm.sophon.engine.kubernetes.spark.deployment.core.AbstractNativeClusterDeployment;
import com.jm.sophon.engine.kubernetes.spark.utils.SystemUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/10/9 19:59
 */
public class SparkNativeClusterDeployment extends AbstractNativeClusterDeployment {

    private static final String SPARK_HOME = "SPARK_HOME";

    public SparkNativeClusterDeployment(SparkSophonContext sparkSophonContext) {
        super(sparkSophonContext);
        this.sparkConfig = sparkSophonContext.getSparkConfig();

        //todo 校验sparkConf,并转成SparkShellModel
    }

    @Override
    protected List<String> buildCommand() {
        List<String> cmdArgs = SystemUtils.buildCmdArgs();
        String command = this.sparkShellModel.buildShell();
        if(StringUtils.isBlank(command)){
            throw new RuntimeException("buildCommand command is blank");
        }
        cmdArgs.add(command);
        return cmdArgs;
    }

    @Override
    public void pre() {
        //todo 1.如果spark启动的服务器不在k8s集群上，需要把k8s集群~/.kube/config文件复制一份到当前主机同样目录下
        if(!checkKubeConfigFileIsExits()){
            LOG.error("kubernetes config not exits");
        }

        //todo 2.检查RBAC






    }

    private Boolean checkKubeConfigFileIsExits() {

        return true;
    }


    @Override
    public void post() {

    }

    @Override
    public void watch() {
        //nothing
    }

}
