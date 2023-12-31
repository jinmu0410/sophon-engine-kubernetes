package com.jm.sophon.engine.kubernetes.spark.deployment;

import cn.hutool.core.collection.CollectionUtil;
import com.jm.sophon.engine.kubernetes.spark.config.KubernetesClientAdapter;
import com.jm.sophon.engine.kubernetes.spark.deployment.core.AbstractClusterDeployment;
import com.jm.sophon.engine.kubernetes.spark.deployment.core.SophonContext;
import com.jm.sophon.engine.kubernetes.spark.deployment.model.SparkDeployMode;
import com.jm.sophon.engine.kubernetes.spark.operator.*;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.StatusDetails;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinitionList;
import io.fabric8.kubernetes.client.CustomResource;


import java.util.List;
import java.util.concurrent.FutureTask;
import java.util.stream.Collectors;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/10/7 11:46
 */
public class SparkOperatorClusterDeployment extends AbstractClusterDeployment<SophonContext> {

    public SparkOperatorClusterDeployment(SparkSophonContext sparkSophonContext) {
        super(sparkSophonContext);
        this.sparkConfig = sparkSophonContext.getSparkConfig();
        kubernetesClientAdapter = new KubernetesClientAdapter(
                sparkConfig.getK8sMasterUrl(),
                sparkConfig.getK8sCarCertData(),
                sparkConfig.getK8sClientCrtData(),
                sparkConfig.getK8sClientKeyData()
        );

        this.sparkApplication = getSparkApplication();
    }

    @Override
    public void pre() {
        LOG.info("spark operator cluster deployment pre method");

        if (CollectionUtil.isEmpty(checkSparkOperatorIsExist())) {
            throw new RuntimeException("spark operator not exist");
        }

        SparkPodSpec driver = SparkPodSpec.Builder()
                .cores(Integer.valueOf(sparkConfig.getDriverCores()))
                .memory(sparkConfig.getDriverMemory())
                .serviceAccount(sparkConfig.getK8sServiceAccount())
                .build();

        SparkPodSpec executor = SparkPodSpec.Builder()
                .cores(Integer.valueOf(sparkConfig.getExecutorCores()))
                .instances(Integer.valueOf(sparkConfig.getNumExecutors()))
                        .memory(sparkConfig.getExecutorMemory())
                        .build();

        SparkApplicationSpec sparkApplicationSpec = SparkApplicationSpec.Builder()
                        .type(sparkConfig.getK8sLanguageType())
                        .mode(SparkDeployMode.CLUSTER.name())
                        .image(sparkConfig.getK8sImage())
                        .imagePullPolicy(sparkConfig.getK8sImagePullPolicy())
                        .mainClass(sparkConfig.getMainClass())
                        .mainApplicationFile(sparkConfig.getMainJar())
                        .arguments(sparkConfig.getArguments())
                        .sparkVersion(sparkConfig.getK8sSparkVersion())
                        .restartPolicy(new RestartPolicy(sparkConfig.getK8sRestartPolicy()))
                        .driver(driver)
                        .executor(executor)
                        .sparkConf(sparkConfig.getConf())
                        .build();

        sparkApplication.setSpec(sparkApplicationSpec);

    }

    @Override
    public void doSubmit() {
        this.kubernetesClientAdapter.getClient().resource(sparkApplication).create();
        LOG.info("spark operator cluster submit kubernetes success");

        //todo 获取提交结果
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public void cancel() {
        try {
            List<StatusDetails> statusDetails = this.kubernetesClientAdapter.getClient().resource(getSparkApplication()).delete();

            if (!checkCancelJobResult(statusDetails)) {
                throw new RuntimeException("spark on kubernetes cancel error");
            }
        } catch (Exception e) {
            throw new RuntimeException("spark on kubernetes cancel error, msg = " + e.getMessage());
        } finally {
            if(this.kubernetesClientAdapter != null){
                this.kubernetesClientAdapter.closeKubernetesClient();
            }
        }
    }

    @Override
    public void post() {
        LOG.info("spark operator cluster deployment post method");
        if(this.kubernetesClientAdapter != null){
            this.kubernetesClientAdapter.closeKubernetesClient();
        }
    }

    @Override
    public void watch() {
        FutureTask processStatusFuture = new FutureTask<>(this::watchStatus, null);

        Thread processStatusThread = new Thread(processStatusFuture, this.sparkApplication.getMetadata().getName() + "-watch-status");
        processStatusThread.setDaemon(true);
        processStatusThread.start();
    }

    private List<CustomResourceDefinition> checkSparkOperatorIsExist() {
        CustomResourceDefinitionList customResourceDefinitionList =
                this.kubernetesClientAdapter.getClient().apiextensions().v1().customResourceDefinitions().list();

        String sparkApplicationCRDName = CustomResource.getCRDName(SparkApplication.class);
        return customResourceDefinitionList.getItems().stream()
                .filter(crd -> crd.getMetadata().getName().equals(sparkApplicationCRDName))
                .collect(Collectors.toList());
    }

    private SparkApplication getSparkApplication() {
        SparkApplication sparkApplication = new SparkApplication();
        ObjectMeta metadata = new ObjectMeta();
        metadata.setName(sparkConfig.getAppName());
        metadata.setNamespace(sparkConfig.getK8sNamespace());
        sparkApplication.setMetadata(metadata);
        return sparkApplication;
    }

}
