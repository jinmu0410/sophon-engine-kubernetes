package com.jm.sophon.engine.kubernetes.flink.operator;

import com.jm.sophon.engine.kubernetes.flink.config.KubernetesClientAdapter;
import com.jm.sophon.engine.kubernetes.flink.operator.spec.*;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.KubernetesClient;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/9/28 17:22
 */
public class FlinkDeploymentTest {

    public static void main(String[] args) {

        operator();

    }

    public static void operator() {
        FlinkDeployment flinkDeployment = getFlinkDeployment("basic-example", "flink-operator");

        // FlinkDeploymentSpec
        FlinkDeploymentSpec flinkDeploymentSpec = new FlinkDeploymentSpec();
        flinkDeploymentSpec.setFlinkVersion(FlinkVersion.v1_16);
        flinkDeploymentSpec.setImage("flink:1.16");
        //config
        Map<String, String> flinkConfiguration = new HashMap<>();
        flinkConfiguration.put("taskmanager.numberOfTaskSlots", "1");
        /**
         * checkpoints
         */
        flinkConfiguration.put("state.checkpoints.dir", "file:///opt/flink/checkpoints");
        /**
         * HA
         */
        flinkConfiguration.put("high-availability.type", "kubernetes");
        flinkConfiguration.put("high-availability", "org.apache.flink.kubernetes.highavailability.KubernetesHaServicesFactory");
        flinkConfiguration.put("high-availability.storageDir", "file:///opt/flink/flink_recovery");

        flinkDeploymentSpec.setFlinkConfiguration(flinkConfiguration);
        flinkDeployment.setSpec(flinkDeploymentSpec);
        flinkDeploymentSpec.setServiceAccount("flink-operator");
        JobManagerSpec jobManagerSpec = new JobManagerSpec();
        jobManagerSpec.setResource(new Resource(1.0, "1024m", "2G"));
        flinkDeploymentSpec.setJobManager(jobManagerSpec);
        TaskManagerSpec taskManagerSpec = new TaskManagerSpec();
        taskManagerSpec.setResource(new Resource(1.0, "1024m", "2G"));
        flinkDeploymentSpec.setTaskManager(taskManagerSpec);

        /**
         * flink pod template
         */
        Pod podTemplate = new Pod();
        podTemplate.setSpec(getPodSpec());

        flinkDeploymentSpec.setPodTemplate(podTemplate);

        flinkDeploymentSpec
                .setJob(
                        JobSpec.builder()
                                .jarURI(
                                        "local:///opt/flink/examples/streaming/StateMachineExample.jar")
                                .parallelism(1)
                                .upgradeMode(UpgradeMode.STATELESS)
                                .build());


        KubernetesClientAdapter kubernetesClientAdapter = new KubernetesClientAdapter();
        KubernetesClient client = kubernetesClientAdapter.getClient();
        //启动flink任务
        FlinkDeployment deployment = client.resource(flinkDeployment).create();
    }

    static FlinkDeployment getFlinkDeployment(String name, String namespace) {
        FlinkDeployment flinkDeployment = new FlinkDeployment();
        flinkDeployment.setApiVersion("flink.apache.org/v1beta1");
        flinkDeployment.setKind("FlinkDeployment");
        ObjectMeta objectMeta = new ObjectMeta();
        objectMeta.setNamespace(namespace);
        objectMeta.setName(name);

        return flinkDeployment;
    }

    static PodSpec getPodSpec() {
        PodSpec podSpec = new PodSpec();
        List<Container> containers = new ArrayList<>();
        containers.add(getPodSpecContainer());

        podSpec.setContainers(containers);

        //volumes
        List<Volume> volumes = new ArrayList<>();
        //log-volume
        Volume log_volume = new Volume();
        log_volume.setName("log-volume");
        log_volume.setPersistentVolumeClaim(new PersistentVolumeClaimVolumeSource("flink-log-pvc ", false));

        //ck-volume
        Volume ck_volume = new Volume();
        ck_volume.setName("checkpoints-volume");
        ck_volume.setPersistentVolumeClaim(new PersistentVolumeClaimVolumeSource("flink-ck-pvc", false));

        //ha-volume
        Volume ha_volume = new Volume();
        ha_volume.setName("ha-volume");
        ha_volume.setPersistentVolumeClaim(new PersistentVolumeClaimVolumeSource("flink-ha-pvc", false));

        volumes.add(log_volume);
        volumes.add(ck_volume);
        volumes.add(ha_volume);

        podSpec.setVolumes(volumes);

        return podSpec;
    }

    static Container getPodSpecContainer() {
        Container container = new Container();
        //env
        List<EnvVar> envs = new ArrayList<>();
        EnvVar envVar = new EnvVar();
        envVar.setAdditionalProperty("TZ", "Asia/Shanghai");
        envs.add(envVar);

        container.setEnv(envs);
        // volumeMounts
        List<VolumeMount> volumeMounts = new ArrayList<>();

        VolumeMount log = new VolumeMount();
        log.setName("log-volume");
        log.setMountPath("/opt/flink/log");

        VolumeMount ck = new VolumeMount();
        ck.setName("checkpoints-volume");
        ck.setMountPath("/opt/flink/checkpoints");

        VolumeMount ha = new VolumeMount();
        ha.setName("ha-volume");
        ha.setMountPath("/opt/flink/ha");

        volumeMounts.add(log);
        volumeMounts.add(ha);
        volumeMounts.add(ck);

        container.setVolumeMounts(volumeMounts);

        return container;
    }
}
