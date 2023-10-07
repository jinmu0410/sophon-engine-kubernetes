package com.jm.sophon.engine.kubernetes.spark.deployment;

import cn.hutool.core.collection.CollectionUtil;
import com.jm.sophon.engine.kubernetes.spark.config.KubernetesClientAdapter;
import com.jm.sophon.engine.kubernetes.spark.deployment.core.AbstractClusterDeployment;
import com.jm.sophon.engine.kubernetes.spark.deployment.core.SophonContext;
import com.jm.sophon.engine.kubernetes.spark.deployment.model.SparkDeployMode;
import com.jm.sophon.engine.kubernetes.spark.operator.RestartPolicy;
import com.jm.sophon.engine.kubernetes.spark.operator.SparkApplication;
import com.jm.sophon.engine.kubernetes.spark.operator.SparkApplicationSpec;
import com.jm.sophon.engine.kubernetes.spark.operator.SparkPodSpec;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.StatusDetails;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinitionList;
import io.fabric8.kubernetes.client.CustomResource;

import java.util.List;
import java.util.stream.Collectors;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/10/7 11:46
 */
public class SparkOperatorClusterDeployment extends AbstractClusterDeployment<SophonContext> {

    public KubernetesClientAdapter kubernetesClientAdapter;

    public SparkApplication sparkApplication;


    public SparkOperatorClusterDeployment(SparkSophonContext sparkSophonContext){
        super(sparkSophonContext);
    }

    @Override
    public void pre() {
        kubernetesClientAdapter = new KubernetesClientAdapter(
                sparkConfig.getK8sMasterUrl(),
                sparkConfig.getK8sCarCertData(),
                sparkConfig.getK8sClientCrtData(),
                sparkConfig.getK8sClientKeyData()
        );

        if (CollectionUtil.isEmpty(checkSparkOperatorIsExist())) {
            throw new RuntimeException("spark operator not exist");
        }

        SparkPodSpec driver = SparkPodSpec.Builder()
                        .cores(sparkConfig.getDriverCores())
                        .memory(sparkConfig.getDriverMemory())
                        .serviceAccount(sparkConfig.getK8sServiceAccount())
                        .build();

        SparkPodSpec executor = SparkPodSpec.Builder()
                        .cores(sparkConfig.getExecutorCores())
                        .instances(sparkConfig.getNumExecutors())
                        .memory(sparkConfig.getExecutorMemory())
                        .build();

        SparkApplicationSpec sparkApplicationSpec = SparkApplicationSpec.Builder()
                        .type(sparkConfig.getK8sLanguageType())
                        .mode(SparkDeployMode.CLUSTER.name())
                        .image(sparkConfig.getK8sImage())
                        .imagePullPolicy(sparkConfig.getK8sImagePullPolicy())
                        .mainClass(sparkConfig.getMainClass())
                        .mainApplicationFile(sparkConfig.getAppResource())
                        .arguments(sparkConfig.getArgs())
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
        SparkApplication application = this.kubernetesClientAdapter.getClient().resource(sparkApplication).create();

        //todo 获取提交结果

    }

    @Override
    public void post() {
        this.kubernetesClientAdapter.closeKubernetesClient();
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
            this.kubernetesClientAdapter.closeKubernetesClient();
        }
    }

    public List<CustomResourceDefinition> checkSparkOperatorIsExist() {
        CustomResourceDefinitionList customResourceDefinitionList =
                this.kubernetesClientAdapter.getClient().apiextensions().v1().customResourceDefinitions().list();

        String sparkApplicationCRDName = CustomResource.getCRDName(SparkApplication.class);
        return customResourceDefinitionList.getItems().stream()
                .filter(crd -> crd.getMetadata().getName().equals(sparkApplicationCRDName))
                .collect(Collectors.toList());
    }

    public SparkApplication getSparkApplication() {
        SparkApplication sparkApplication = new SparkApplication();
        ObjectMeta metadata = new ObjectMeta();
        metadata.setName(sparkConfig.getAppName());
        metadata.setNamespace(sparkConfig.getK8sNamespace());
        sparkApplication.setMetadata(metadata);
        return sparkApplication;
    }

    public Boolean checkCancelJobResult(List<StatusDetails> statusDetails) {
        Boolean result = false;
        if (CollectionUtil.isNotEmpty(statusDetails)) {
            StatusDetails details = statusDetails.get(0);
            if (CustomResource.getCRDName(SparkApplication.class).equalsIgnoreCase(details.getKind()) && sparkApplication.getMetadata().getName().equalsIgnoreCase(details.getName())) {
                result = true;
            }
        }
        return result;
    }
}
