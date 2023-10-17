package com.jm.sophon.engine.kubernetes;


import cn.hutool.json.JSONUtil;
import com.jm.sophon.engine.kubernetes.spark.config.KubernetesClientAdapter;
import com.jm.sophon.engine.kubernetes.spark.deployment.core.AbstractNativeClusterDeployment;
import com.jm.sophon.engine.kubernetes.spark.operator.*;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.StatusDetails;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/10/3 15:15
 */
public class SparkApplicationTest {

    protected static final Logger LOG = LoggerFactory.getLogger(SparkApplicationTest.class);


    public volatile static Boolean flag = false;

    public static void main(String[] args) {

        submit();
        //cancel();
    }


    public static void submit(){
        SparkApplication sparkApplication = getSparkApplication();

        SparkPodSpec driver =
                SparkPodSpec.Builder()
                        .cores(1)
                        .memory("1G")
                        .serviceAccount("my-release-test-spark")
                        .build();

        SparkPodSpec executor =
                SparkPodSpec.Builder()
                        .cores(1)
                        .instances(1)
                        .memory("1G")
                        .serviceAccount("my-release-test-spark")
                        .build();

        Map<String, String> sparkConfMap = new HashMap<>();

        SparkApplicationSpec sparkApplicationSpec =
                SparkApplicationSpec.Builder()
                        .type("Scala")
                        .mode("cluster")
                        .image("apache/spark:3.3.1")
                        .imagePullPolicy("Always")
                        .mainClass("org.apache.spark.examples.SparkPi")
                        .mainApplicationFile("local:///opt/spark/examples/jars/spark-examples_2.12-3.3.1.jar")
                        .sparkVersion("3.3.1")
                        .restartPolicy(new RestartPolicy("Never"))
                        .driver(driver)
                        .executor(executor)
                        .sparkConf(sparkConfMap)
                        .build();

        sparkApplication.setSpec(sparkApplicationSpec);

        KubernetesClientAdapter kubernetesClientAdapter = new KubernetesClientAdapter();
        KubernetesClient client = kubernetesClientAdapter.getClient();

        client.resource(sparkApplication).create();

        AtomicBoolean flag = new AtomicBoolean(false);
        Watch watch = testWatch(client,flag);

        //这个必须，不然watch方法没有日志打印
//        try {
//            System.in.read();
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }

        while (true){
            if(flag.get()){
                System.out.println("进入结束标识");
                watch.close();
                break;
            }
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        //kubernetesClientAdapter.closeKubernetesClient();
    }

    public static SparkApplication getSparkApplication(){
        SparkApplication sparkApplication = new SparkApplication();
        ObjectMeta metadata = new ObjectMeta();
        metadata.setName("test-spark-operator-123456");
        metadata.setNamespace("spark-operator-test");
        sparkApplication.setMetadata(metadata);

        return sparkApplication;
    }

    public static void cancel(){
        SparkApplication sparkApplication = getSparkApplication();

        KubernetesClientAdapter kubernetesClientAdapter = new KubernetesClientAdapter();
        KubernetesClient client = kubernetesClientAdapter.getClient();

        List<StatusDetails> statusDetails = client.resource(sparkApplication).delete();

        kubernetesClientAdapter.closeKubernetesClient();
        LOG.info("spark 删除成功");

    }

    public static Watch testWatch(KubernetesClient client, AtomicBoolean flag){
        return client.resource(getSparkApplication()).inNamespace("spark-operator-test").watch(new Watcher<SparkApplication>() {
            @Override
            public void eventReceived(Action action, SparkApplication resource) {
                System.out.println("action = " + action);
                System.out.println("SparkApplication status = " + JSONUtil.toJsonStr(resource.getStatus()));

                LOG.info("action = " + action);
                LOG.info("SparkApplication status = " + JSONUtil.toJsonStr(resource.getStatus()));


                if (action.equals(Action.MODIFIED)) {
                    SparkApplicationStatus status = resource.getStatus();
                    if (status != null) {
                        ApplicationState applicationState = status.getApplicationState();
                        if (applicationState.getState().equalsIgnoreCase("COMPLETED")) {
                            System.out.println("任务已完成");
                            LOG.info("任务已经完成");
                            //停止
                            cancel();
                            flag.getAndSet(true);
                            throw new RuntimeException("sparkApplication completed");
                        }
                    }
                }
            }

            @Override
            public void onClose(WatcherException cause) {
                System.out.println("WatcherException = " + cause.getMessage());
                LOG.info("WatcherException = " +cause.getMessage());
            }
        });
    }
}
