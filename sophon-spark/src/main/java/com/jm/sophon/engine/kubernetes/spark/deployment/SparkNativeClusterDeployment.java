package com.jm.sophon.engine.kubernetes.spark.deployment;

import com.jm.sophon.engine.kubernetes.spark.config.KubernetesClientAdapter;
import com.jm.sophon.engine.kubernetes.spark.deployment.core.AbstractNativeClusterDeployment;
import com.jm.sophon.engine.kubernetes.spark.deployment.model.SparkShellModel;
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

        this.sparkShellModel = buildSparkShellModel();
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
        this.kubernetesClientAdapter = new KubernetesClientAdapter();
        kubernetesClientAdapter.init();
        if(this.kubernetesClientAdapter.getClient() != null){
            return true;
        }
        return false;
    }

    private SparkShellModel buildSparkShellModel() {
        SparkShellModel.SparkDriverModel sparkDriverModel = SparkShellModel.SparkDriverModel.builder()
                .driverCores(this.sparkConfig.getDriverCores())
                .driverMemory(this.sparkConfig.getDriverMemory())
                .driverClassPath(this.sparkConfig.getDriverClassPath())
                .driverJavaOptions(this.sparkConfig.getDriverJavaOptions())
                .driverLibraryPath(this.sparkConfig.getDriverLibraryPath())
                .build();

        SparkShellModel.SparkExecutorModel sparkExecutorModel = SparkShellModel.SparkExecutorModel.builder()
                .executorCores(this.sparkConfig.getExecutorCores())
                .executorMemory(this.sparkConfig.getExecutorMemory())
                .numExecutors(this.sparkConfig.getNumExecutors())
                .totalExecutorCores(this.sparkConfig.getTotalExecutorCores())
                .build();

        SparkShellModel.SparkKubernetesModel sparkKubernetesModel = SparkShellModel.SparkKubernetesModel.builder()
                .k8sNamespace(this.sparkConfig.getK8sNamespace())
                .k8sImage(this.sparkConfig.getK8sImage())
                .k8sImagePullPolicy(this.sparkConfig.getK8sImagePullPolicy())
                .k8sServiceAccount(this.sparkConfig.getK8sServiceAccount())
                .build();

        return SparkShellModel.builder()
                .sparkHome(this.sparkConfig.getSparkHome())
                .master(this.sparkConfig.getK8sMasterUrl())
                .name(this.sparkConfig.getAppName())
                .deployMode(this.sparkConfig.getDeployMode())
                .files(this.sparkConfig.getFiles())
                .archives(this.sparkConfig.getArchives())
                .jars(this.sparkConfig.getJars())
                .sparkDriverModel(sparkDriverModel)
                .sparkExecutorModel(sparkExecutorModel)
                .sparkKubernetesModel(sparkKubernetesModel)
                .mainClass(this.sparkConfig.getMainClass())
                .mainJarPath(this.sparkConfig.getMainJar())
                .arguments(this.sparkConfig.getArguments())
                .build();
    }


    @Override
    public void post() {
        //noting
    }

    @Override
    public void watch() {
        //nothing
    }

}
