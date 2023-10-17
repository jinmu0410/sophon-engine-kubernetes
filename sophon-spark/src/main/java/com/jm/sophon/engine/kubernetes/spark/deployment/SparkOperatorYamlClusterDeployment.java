package com.jm.sophon.engine.kubernetes.spark.deployment;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.jm.sophon.engine.kubernetes.spark.config.KubernetesClientAdapter;
import com.jm.sophon.engine.kubernetes.spark.deployment.core.AbstractClusterDeployment;
import com.jm.sophon.engine.kubernetes.spark.deployment.core.SophonContext;
import com.jm.sophon.engine.kubernetes.spark.operator.SparkApplication;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.StatusDetails;
import io.fabric8.kubernetes.client.dsl.WritableOperation;

import java.util.List;


/**
 * TODO
 * yaml文件方式提交
 *
 * @Author jinmu
 * @Date 2023/10/8 11:15
 */
public class SparkOperatorYamlClusterDeployment extends AbstractClusterDeployment<SophonContext> {

    private final static String OPERATOR_ITEM = "item";
    private final static String OPERATOR_KIND = "kind";
    private final static String OPERATOR_metadata = "metadata";
    private final static String OPERATOR_NAME = "name";
    private final static String OPERATOR_NAMESPACE = "namespace";

    public SparkOperatorYamlClusterDeployment(SparkSophonContext sparkSophonContext) {
        super(sparkSophonContext);
        this.sparkConfig = sparkSophonContext.getSparkConfig();
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
            throw new RuntimeException("spark on kubernetes yamlContent style check error, yamlContent = " + this.sparkConfig.getYamlContent());
        }
    }

    @Override
    public void doSubmit() {
        kubernetesClientAdapter.getClient().resource(this.sparkConfig.getYamlContent()).create();

        LOG.info("spark operator yaml cluster submit kubernetes success");
        //todo
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void post() {
        LOG.info("spark operator yaml cluster deployment post method");
    }

    @Override
    public void cancel() {
        try {
            List<StatusDetails> statusDetails = this.kubernetesClientAdapter.getClient().resource(this.sparkConfig.getYamlContent()).delete();

            if (!checkCancelJobResult(statusDetails)) {
                throw new RuntimeException("spark on kubernetes cancel error");
            }
        } catch (Exception e) {
            throw new RuntimeException("cancel error msg = " + e.getMessage());
        } finally {
            if(this.kubernetesClientAdapter != null){
                this.kubernetesClientAdapter.closeKubernetesClient();
            }
        }
    }

    private Boolean checkYamlContentRightful() {
        Boolean result = false;
        try {
            // –dry-run 校验yaml文件格式
            WritableOperation<HasMetadata> writableOperation = kubernetesClientAdapter.getClient().resource(this.sparkConfig.getYamlContent()).dryRun();

            if (writableOperation != null) {
                JSONObject entries = JSONUtil.parseObj(writableOperation);
                JSONObject item = entries.getJSONObject(OPERATOR_ITEM);

                if (item.getStr(OPERATOR_KIND).equalsIgnoreCase(SparkApplication.Kind)) {
                    JSONObject metadataJson = item.getJSONObject(OPERATOR_metadata);
                    //封装sparkApplication
                    ObjectMeta metadata = new ObjectMeta();
                    metadata.setName(metadataJson.getStr(OPERATOR_NAME));
                    metadata.setNamespace(metadataJson.getStr(OPERATOR_NAMESPACE));
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
}
