package com.jm.sophon.engine.kubernetes;

import cn.hutool.json.JSONUtil;
import com.jm.sophon.engine.kubernetes.spark.config.KubernetesClientAdapter;
import com.jm.sophon.engine.kubernetes.spark.deployment.SparkOperatorYamlClusterDeployment;
import com.jm.sophon.engine.kubernetes.spark.deployment.SparkSophonContext;
import com.jm.sophon.engine.kubernetes.spark.deployment.model.SparkConfig;
import com.jm.sophon.engine.kubernetes.spark.operator.SparkApplication;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.StatusDetails;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.KubernetesClient;

import java.util.List;


/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/10/17 14:28
 */
public class SparkOperatorYamlTest {

    public static void main(String[] args) {
        System.out.println(CustomResource.getCRDName(SparkApplication.class));

        //testYaml();
        //cancel();
    }

    static void testYaml(){
        String yamlContent = "apiVersion: \"sparkoperator.k8s.io/v1beta2\"\n" +
                "kind: SparkApplication\n" +
                "metadata:\n" +
                "  name: spark-pi\n" +
                "  namespace: spark-operator-test\n" +
                "spec:\n" +
                "  type: Scala\n" +
                "  mode: cluster\n" +
                "  image: \"registry.cn-hangzhou.aliyuncs.com/public-namespace/spark:v3.1.1\"\n" +
                "  imagePullPolicy: Always\n" +
                "  mainClass: org.apache.spark.examples.SparkPi\n" +
                "  mainApplicationFile: \"local:///opt/spark/examples/jars/spark-examples_2.12-3.1.1.jar\"\n" +
                "  sparkVersion: \"3.1.1\"\n" +
                "  restartPolicy:\n" +
                "    type: Never\n" +
                "  driver:\n" +
                "    cores: 1\n" +
                "    coreLimit: \"1200m\"\n" +
                "    memory: \"512m\"\n" +
                "    labels:\n" +
                "      version: 3.1.1\n" +
                "    serviceAccount: my-release-test-spark\n" +
                "  executor:\n" +
                "    cores: 1\n" +
                "    instances: 1\n" +
                "    memory: \"512m\"\n" +
                "    labels:\n" +
                "      version: 3.1.1";

        String carCertData = "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUM1ekNDQWMrZ0F3SUJBZ0lCQURBTkJna3Foa2lHOXcwQkFRc0ZBREFWTVJNd0VRWURWUVFERXdwcmRXSmwKY201bGRHVnpNQjRYRFRJek1EVXlOVEEzTkRZek5Wb1hEVE16TURVeU1qQTNORFl6TlZvd0ZURVRNQkVHQTFVRQpBeE1LYTNWaVpYSnVaWFJsY3pDQ0FTSXdEUVlKS29aSWh2Y05BUUVCQlFBRGdnRVBBRENDQVFvQ2dnRUJBTGVDClI4eUxPdVhKRmx4UWRyMWFXem5PNkJSQ3FUVTZSVmVlRXNRQlVJNVU1RnB3SVcxdTk2TGRGT2NrN2svSm1mQloKLyt4bFJRSDkrUjEvTGdtWUUzVkFmM3NNVUJGSk42b0E0NndVWmtjWnVnVnNaNVI5NXNUQ1o3Yk1lazNic2d0ZwpEUGZadzdMWnJmYURvNHhaRXVnRjBPNUtiOXdPQ1BmcnpSd2N2U1laUlFnMUJBU1ExYnJ3bi9sL2JuZmN3Mk1QCnNSUC9IS3dyWCtSSTFKYmZUdmdJV3prTzJ5WFJLSWsyamhEd1o2Z2dsZnZPRXBqS3UyQkpUWEh0L3pKZ3RITmYKZEE3WWZTQ0pNOExCT293K1NjRzBySFRlOHpFYTRKVHpjL2xzcVJnbjlaSDJ1VWIyNlZXOUJQTEJ5V0gycGxEVgpnL2xiVitEOGQyMm5YOXZJN0wwQ0F3RUFBYU5DTUVBd0RnWURWUjBQQVFIL0JBUURBZ0trTUE4R0ExVWRFd0VCCi93UUZNQU1CQWY4d0hRWURWUjBPQkJZRUZJMGtDVGN0QzVPb2FZQmJKNVh6cEJ6VGdsRlhNQTBHQ1NxR1NJYjMKRFFFQkN3VUFBNElCQVFBSkRCMHVFYmEvMTRmVFBjMi9taXE5TURmN0NZYUplUEUzWXZDMUpkZzV0UktKZWQ1dQo5ZGY5NWg0bXhmQTZLVFNzZU5JOFJrSzMrcGlDTnhvYjFHWXFuK3ZYWW1WOWhFdE9lcmhnd2k4YWdPVEN0eEl6CjRxK251R0I4amFtdU42bTV0WUt5YjZmQWNPM0ZtTWVLMXdrcU5sUHlYQTZCUzc5RVlSR0xpNEVNQ09VZEdIU3cKSU5WV3VneFBLYTRaNVFPZDZrWjZ6enFCQ0s2SlltN3NDbERwbmVQSDJETlRXZlJaNHJUZUlRdUNrbk5FTlRGMwpQMlVuaTFuQy9QdEtlOHJ4WEx4cE80WVloSDVhc0M0QUNuOTgvZWRDNG1JdEVtMjUvK0pkbHo3Ukh1WTZicURMCkFCdThxSFBoMXVxdjczeDl3c04rWUZBWjRhdTNFNUJRSFpJMQotLS0tLUVORCBDRVJUSUZJQ0FURS0tLS0tCg";
        String clientCrtData = "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSURCVENDQWUyZ0F3SUJBZ0lRQkhibk04cjVSbGdmSWVKMHoxOFRwREFOQmdrcWhraUc5dzBCQVFzRkFEQVYKTVJNd0VRWURWUVFERXdwcmRXSmxjbTVsZEdWek1CNFhEVEl6TURVeU5UQTNOVEV6TkZvWERUTXpNRFV5TWpBMwpORFl6TlZvd0VERU9NQXdHQTFVRUF4TUZZV1J0YVc0d2dnRWlNQTBHQ1NxR1NJYjNEUUVCQVFVQUE0SUJEd0F3CmdnRUtBb0lCQVFDYmM2bVJvc3N4QWRNZHZ3VldxMEl4SFh6RzdET2xWQzduYWxwMkIvaUJVbUhiMGIzVzkycEcKS0Zvb21KRkl2MjZGenJKc0NUbmdzUE5lUGdHUGhNTmU1M2lqMXJ3eFVFbmhMZmlWQzMyRTRoK2hsOHR5NVBSNgozRm11MDA4VUFuWFlSTEl6K3Z6ZW5vdXhEMVpDdzZjSEFMT3RXZVVzT2NYeS83SHE5dUR0OU0yblVQZjhlczJSCk1FWnhhOHBsQVo5OHcrclJvZ3UxRWh0eDhsaHJTZ3hxSGo3V1A1MnNXcHJHOVIvMHhkR2pVT0NFYk5tcEExeTMKaXdDcjB2dlNBeWpkbGIxOVp4eTg4aDRkL3BKOXRrQWR4RFRJd1pZYkp0Qk05N3RPTUJ5OUVWNkkxMS9XcXBTcwpqUlRhclBaMjNXUWdQN0JCRWtsVGJaWThLZDRpMWpCSEFnTUJBQUdqVmpCVU1BNEdBMVVkRHdFQi93UUVBd0lGCm9EQVRCZ05WSFNVRUREQUtCZ2dyQmdFRkJRY0RBakFNQmdOVkhSTUJBZjhFQWpBQU1COEdBMVVkSXdRWU1CYUEKRkkwa0NUY3RDNU9vYVlCYko1WHpwQnpUZ2xGWE1BMEdDU3FHU0liM0RRRUJDd1VBQTRJQkFRQlZpZVFGS2RCYwp6b2srSEh3TkpwU1h2MlloMjhBSzhISm5Fa3lqRm50UEhzc0xPalNjdWltaWp6aHUxUElYdENjN042YjhFd2xMCjIwc1pMN2xqYVIrQU1YSzBvNDBIVVMvTmVSWnFJYmxBaWlwcjNJSnZNYlR0NGFtd2N6a21Jb0dZZ0taUmtBQ20KY2RycWUzcnRnTjdjSXZ1ekc0L2w3cjNydWU3UGhFOE5VaXZudlhqYjVCc29RWk1oWFN5b1NQSVV2ajAySGt4Tgp0SzUzU01GS3pMSklxMVRwRHdhYVdycGdXNURCNWRaWnNMMFVBRDJMN0JXaUlLcTFXeElkMEk5T0J4WUpOMFd1ClVMSTZNU2dId2p6enQvNStZOWlVcldiOEIyVDh5aDZXR1VIOWdtWTVKbGVzMVNKQi83bVRDaGFBZUdqSFpzUTEKTmRLYjVkN3FrczhECi0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0K";
        String clientKeyData = "LS0tLS1CRUdJTiBSU0EgUFJJVkFURSBLRVktLS0tLQpNSUlFcEFJQkFBS0NBUUVBbTNPcGthTExNUUhUSGI4RlZxdENNUjE4eHV3enBWUXU1MnBhZGdmNGdWSmgyOUc5CjF2ZHFSaWhhS0ppUlNMOXVoYzZ5YkFrNTRMRHpYajRCajRURFh1ZDRvOWE4TVZCSjRTMzRsUXQ5aE9JZm9aZkwKY3VUMGV0eFpydE5QRkFKMTJFU3lNL3I4M3A2THNROVdRc09uQndDenJWbmxMRG5GOHYreDZ2Ymc3ZlROcDFEMwovSHJOa1RCR2NXdktaUUdmZk1QcTBhSUx0UkliY2ZKWWEwb01haDQrMWorZHJGcWF4dlVmOU1YUm8xRGdoR3paCnFRTmN0NHNBcTlMNzBnTW8zWlc5ZldjY3ZQSWVIZjZTZmJaQUhjUTB5TUdXR3liUVRQZTdUakFjdlJGZWlOZGYKMXFxVXJJMFUycXoyZHQxa0lEK3dRUkpKVTIyV1BDbmVJdFl3UndJREFRQUJBb0lCQVFDTzRDaXJOWEFDRkFaQgpJYys3VDk2Zm05V1NVNGhJWFc1YXZoSjB4M0N0RTlYam53d1g1d0VqaUhVTk9PVFhjek1YQXRwVWw4bzliUVhSCnliWFBmaHUvUDRwUE04OTJsNisvcW12K09UaGRpU08vZHUvRUl4RmRWdWVLMnFhazRub0RrMmdpaEg0ajhwNjcKMXpmV1YxTk9DV1FiWVROVWlha1paYy9XQUtETnN6TGU3bCtKcEJydU9HbUdHS0l0T1Fkc1FpVmliUlpycHNhUQo5YW5JOTNJTXVvK1VCMnBhMDRHUlZPTnpCSzVHUldSL0RwU0ZVUlJHTkg5WFJRNGRzYnlvZjRZMkZVWFRIdlljCk42aTVXU0tDS3A0M2xJcUMzMzJ2TjlFVHoyZlJ2K0JyN1MwLzVlc0tpVEZHdWpicG9iRFJ5bS9Ec1VzZFQwWjgKZmJiNkRpUXhBb0dCQU02anRuMk5VR21yZXdsSnRsSzRWb1hFTkFOVVkvaDBlZU5uUkxRMkZsTjNmNnFMSmVLTQovTTAvK085bFNoMlJSNTBIV29UOW83TEdaU2xZa2dlMVJzTWppM2ljQjNzUWdQeGg3bVp2SVdOSUY4V0htc0VhCmExRHZ3Q003YzZaOVB0bENjUlFzTnNOY2ZwYjY3Nno0blpUWDdQc3l0cVJMbS92bUVIRnF0M3k1QW9HQkFNQ1YKd0lvWmlhWFRsSVgzODJGd1lZVjRjcm1pSVhLZGlxNkNCc0hIOGg1NjRIUUtyS3BRQ3VFRzdoZmlvRFczc004VwpTd1BiRUp0UDdKdW0wdzhqb21QV3RjRWZkY1pJRjJJVExsc2lhSStmamFNbWtaODJmSllhWExtbmVOdkFmQWxFCnkzZHcwMThBa2x5MUNIbkphOHFQQm53cUxhb2NhcjJRMUZueXBwVC9Bb0dBQ2tLS3J4VXpKdXdDd2VWTkRqSmsKNldOaWliL0k3dThwbGlic2ZGNHJJWjNQVXZKeHdSdnErVzN4dUVFNU90cVp5YXd4ekdTam1oN0xxdy9nd0UwNQo1RHFCbEUxNitadEtMRDNZam5GNklPblZkRk5WVmIwd0V1YTBqWXRJSkw0WFBzWlR3d0ttL1hBOFFOYll5NmZVCmV5MTFjTXVGMGdhSENpelVLQzE2U3RFQ2dZQUJiTnBJNjFsMmJiTnFybVFSczRnMU9hZjNzYmgzcGF6U0Q4cEgKQXRqSzJ2UHdSK2Z4TnJodDQrUTlMd01xZ3BwL1hkWGVTRysrQm9Ca3duNUxYV01sUkFDQ1ZsNUR3bUxSNmZ4cwoxanRaM0w5cWxxMTdOU2NFNFZzUVVLbjNUbHhjb1RLMFJwUjUzb0kyeDJ3eG9vajJyY3BKZnZKcGtONUZXdDRICmNHRkZOd0tCZ1FERlp6b3ljTXR1cDhzUUZUcEVsVU5WMkZ5RkFLUTk1VWwrMnJZTTJwTDl2L2V3ak44YzllNVIKTG5pTldVek4vZWlCVnBqR24xb0psRERYK3hxYWJTeWF6dnBIcVFwVjVsTlFKNVRYMGoxQ0Y1aUZEbkxBSS9VbwpodjFIZW1xVjVsOERZWUVVZkhhbUQyTVhXWHFTSlBuQVd3OTlKMTlUeWhIaWZMeWh1eEpOTEE9PQotLS0tLUVORCBSU0EgUFJJVkFURSBLRVktLS0tLQo";

        String k8sMasterUrl = "https://192.168.217.140:6443";

        SparkSophonContext sparkSophonContext = new SparkSophonContext();

        SparkConfig sparkConfig = SparkConfig.builder()
                .yamlContent(yamlContent)
                .k8sMasterUrl(k8sMasterUrl)
                .k8sCarCertData(carCertData)
                .k8sClientCrtData(clientCrtData)
                .k8sClientKeyData(clientKeyData)
                .build();

        sparkSophonContext.setSparkConfig(sparkConfig);

        SparkOperatorYamlClusterDeployment sparkOperatorClusterDeployment = new SparkOperatorYamlClusterDeployment(sparkSophonContext);

        //sparkOperatorClusterDeployment.submit();

        sparkOperatorClusterDeployment.cancel();
    }

    static void cancel(){

        String yamlContent = "apiVersion: \"sparkoperator.k8s.io/v1beta2\"\n" +
                "kind: SparkApplication\n" +
                "metadata:\n" +
                "  name: spark-pi\n" +
                "  namespace: spark-operator-test\n" +
                "spec:\n" +
                "  type: Scala\n" +
                "  mode: cluster\n" +
                "  image: \"registry.cn-hangzhou.aliyuncs.com/public-namespace/spark:v3.1.1\"\n" +
                "  imagePullPolicy: Always\n" +
                "  mainClass: org.apache.spark.examples.SparkPi\n" +
                "  mainApplicationFile: \"local:///opt/spark/examples/jars/spark-examples_2.12-3.1.1.jar\"\n" +
                "  sparkVersion: \"3.1.1\"\n" +
                "  restartPolicy:\n" +
                "    type: Never\n" +
                "  driver:\n" +
                "    cores: 1\n" +
                "    coreLimit: \"1200m\"\n" +
                "    memory: \"512m\"\n" +
                "    labels:\n" +
                "      version: 3.1.1\n" +
                "    serviceAccount: my-release-test-spark\n" +
                "  executor:\n" +
                "    cores: 1\n" +
                "    instances: 1\n" +
                "    memory: \"512m\"\n" +
                "    labels:\n" +
                "      version: 3.1.1";

        KubernetesClientAdapter kubernetesClientAdapter = new KubernetesClientAdapter();
        KubernetesClient client = kubernetesClientAdapter.getClient();

        client.resource(yamlContent).create();

//        List<StatusDetails> statusDetails = client.resource(yamlContent).delete();
//
//        System.out.println(JSONUtil.toJsonStr(statusDetails));

    }
}
