package com.jm.sophon.engine.kubernetes.spark.deployment;

import cn.hutool.core.collection.CollectionUtil;
import com.jm.sophon.engine.kubernetes.spark.config.KubernetesClientAdapter;
import com.jm.sophon.engine.kubernetes.spark.deployment.core.AbstractClusterDeployment;
import com.jm.sophon.engine.kubernetes.spark.deployment.core.SophonContext;
import com.jm.sophon.engine.kubernetes.spark.operator.SparkApplication;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.StatusDetails;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.dsl.WritableOperation;

import java.util.List;


/**
 * TODO
 * yaml文件方式提交
 * @Author jinmu
 * @Date 2023/10/8 11:15
 */
public class SparkOperatorYamlClusterDeployment extends AbstractClusterDeployment<SophonContext> {
    KubernetesClientAdapter kubernetesClientAdapter;

    HasMetadata hasMetadata;
    String yamlContent;


    public SparkOperatorYamlClusterDeployment(SparkSophonContext sparkSophonContext){
        super(sparkSophonContext);
    }

    @Override
    public void pre() {
       this.yamlContent = sparkConfig.getYamlContent();
        kubernetesClientAdapter = new KubernetesClientAdapter(
                sparkConfig.getK8sMasterUrl(),
                sparkConfig.getK8sCarCertData(),
                sparkConfig.getK8sClientCrtData(),
                sparkConfig.getK8sClientKeyData()
        );

        // –dry-run 校验yaml文件格式
        WritableOperation<HasMetadata> hasMetadataWritableOperation = kubernetesClientAdapter.getClient().resource(this.yamlContent).dryRun();

    }

    @Override
    public void doSubmit() {
        //提交
        this.hasMetadata = kubernetesClientAdapter.getClient().resource(this.yamlContent).create();
    }

    @Override
    public void post() {
        kubernetesClientAdapter.closeKubernetesClient();
    }

    @Override
    public void cancel() {
        try {
            List<StatusDetails> statusDetails = this.kubernetesClientAdapter.getClient().resource(this.yamlContent).delete();

            if (!checkCancelJobResult(statusDetails)) {
                throw new RuntimeException("spark on kubernetes cancel error");
            }
        } catch (Exception e) {
            throw new RuntimeException("spark on kubernetes cancel error, msg = " + e.getMessage());
        } finally {
            this.kubernetesClientAdapter.closeKubernetesClient();
        }
    }


    public Boolean checkCancelJobResult(List<StatusDetails> statusDetails) {
        Boolean result = false;
        if (CollectionUtil.isNotEmpty(statusDetails)) {
            StatusDetails details = statusDetails.get(0);
            if (CustomResource.getCRDName(SparkApplication.class).equalsIgnoreCase(details.getKind()) && this.hasMetadata.getMetadata().getName().equalsIgnoreCase(details.getName())) {
                result = true;
            }
        }
        return result;
    }
}
