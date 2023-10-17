package com.jm.sophon.engine.kubernetes.spark.deployment;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.jm.sophon.engine.kubernetes.spark.config.KubernetesClientAdapter;
import com.jm.sophon.engine.kubernetes.spark.deployment.core.AbstractClusterDeployment;
import com.jm.sophon.engine.kubernetes.spark.deployment.core.SophonContext;
import com.jm.sophon.engine.kubernetes.spark.operator.SparkApplication;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
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
    String yamlContent;

    public SparkOperatorYamlClusterDeployment(SparkSophonContext sparkSophonContext){
        super(sparkSophonContext);
        this.sparkConfig = sparkSophonContext.getSparkConfig();
        this.yamlContent = sparkConfig.getYamlContent();
        kubernetesClientAdapter = new KubernetesClientAdapter(
                sparkConfig.getK8sMasterUrl(),
                sparkConfig.getK8sCarCertData(),
                sparkConfig.getK8sClientCrtData(),
                sparkConfig.getK8sClientKeyData()
        );

        this.sparkApplication = new SparkApplication();
    }

    @Override
    public void pre() {
        LOG.info("spark operator yaml cluster deployment pre method");

        if (!checkYamlContentRightful()) {
            throw new RuntimeException("spark on kubernetes yamlContent style check error, yamlContent = " + this.yamlContent);
        }
    }

    @Override
    public void doSubmit() {
        kubernetesClientAdapter.getClient().resource(this.yamlContent).create();

        LOG.info("spark operator yaml cluster submit kubernetes success");
        //todo
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void post() {
        LOG.info("spark operator yaml cluster deployment post method");
    }

    public Boolean checkYamlContentRightful() {
        Boolean result = false;
        try {
            // –dry-run 校验yaml文件格式
            WritableOperation<HasMetadata> writableOperation = kubernetesClientAdapter.getClient().resource(this.yamlContent).dryRun();

            if (writableOperation != null) {
                JSONObject entries = JSONUtil.parseObj(writableOperation);
                JSONObject item = entries.getJSONObject("item");

                if (item.getStr("kind").equalsIgnoreCase(SparkApplication.Kind)) {
                    JSONObject metadataJson = item.getJSONObject("metadata");
                    //封装sparkApplication
                    ObjectMeta metadata = new ObjectMeta();
                    metadata.setName(metadataJson.getStr("name"));
                    metadata.setNamespace(metadataJson.getStr("namespace"));
                    this.sparkApplication.setMetadata(metadata);

                    result = true;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            return result;
        }
    }

    @Override
    public void cancel() {
        try {
            List<StatusDetails> statusDetails = this.kubernetesClientAdapter.getClient().resource(this.yamlContent).delete();

            if (!checkCancelJobResult(statusDetails)) {
                throw new RuntimeException("spark on kubernetes cancel error");
            }
        } catch (Exception e) {
            throw new RuntimeException("msg = " + e.getMessage());
        } finally {
            this.kubernetesClientAdapter.closeKubernetesClient();
        }
    }


    public Boolean checkCancelJobResult(List<StatusDetails> statusDetails) {
        Boolean result = false;
        if (CollectionUtil.isNotEmpty(statusDetails)) {
            StatusDetails details = statusDetails.get(0);
            if (CustomResource.getCRDName(SparkApplication.class).equalsIgnoreCase(details.getKind() + details.getGroup()) && this.sparkApplication.getMetadata().getName().equalsIgnoreCase(details.getName())) {
                result = true;
            }
        }
        return result;
    }
}
