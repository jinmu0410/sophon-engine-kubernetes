package com.jm.sophon.engine.kubernetes.spark.deployment.core;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.json.JSONUtil;
import com.jm.sophon.engine.kubernetes.spark.config.KubernetesClientAdapter;
import com.jm.sophon.engine.kubernetes.spark.deployment.model.SparkConfig;
import com.jm.sophon.engine.kubernetes.spark.operator.ApplicationState;
import com.jm.sophon.engine.kubernetes.spark.operator.SparkApplication;
import com.jm.sophon.engine.kubernetes.spark.operator.SparkApplicationStatus;
import io.fabric8.kubernetes.api.model.StatusDetails;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/10/7 10:48
 */
public abstract class AbstractClusterDeployment<T extends SophonContext> implements Deployment {
    protected SparkConfig sparkConfig;
    protected static final Logger LOG = LoggerFactory.getLogger(AbstractClusterDeployment.class);
    protected SparkApplication sparkApplication;
    protected KubernetesClientAdapter kubernetesClientAdapter;
    protected AtomicBoolean flag = new AtomicBoolean(false);
    protected T t;
    protected Boolean isStreaming = false;

    public AbstractClusterDeployment(T t) {
        this.t = t;
    }

    public abstract void pre();

    public abstract void doSubmit();

    public abstract void post();
    public abstract void watch();

    @Override
    public void submit() {
        try {
            pre();

            doSubmit();

            watch();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            post();
        }
    }

    protected Watch getSparkApplicationWatch() {
        return this.kubernetesClientAdapter.getClient().resource(this.sparkApplication).inNamespace(this.sparkApplication.getMetadata().getNamespace())
                .watch(new Watcher<SparkApplication>() {
                    @Override
                    public void eventReceived(Action action, SparkApplication resource) {
                        //handleStatus(action, resource);
                        LOG.info("action = " + action);
                        if (resource.getStatus() != null) {
                            LOG.info("resource = " + JSONUtil.toJsonStr(resource.getStatus()));
                        }

                        if (action.equals(Action.MODIFIED) || action.equals(Action.DELETED)) {
                            SparkApplicationStatus status = resource.getStatus();
                            if (status != null) {
                                ApplicationState applicationState = status.getApplicationState();
                                if (applicationState.getState().equalsIgnoreCase("COMPLETED")) {

                                    //todo 这里还可以判断是否需要删除已经完成的任务
                                    flag.getAndSet(true);
                                    throw new RuntimeException("sparkApplication watch kill");
                                }
                            }
                        }
                    }

                    @Override
                    public void onClose(WatcherException cause) {
                        LOG.info("sparkApplicationWatch onclose message = " + cause.getMessage());
                    }
                });
    }

    protected void watchStatus() {
        Watch watch = getSparkApplicationWatch();
        LOG.info("watch status start");
        while (true) {
            if (flag.get()) {
                LOG.info("sparkApplication watchStatus completed");
                watch.close();
                break;
            }
        }
    }

    protected Boolean checkCancelJobResult(List<StatusDetails> statusDetails) {
        Boolean result = false;
        if (CollectionUtil.isNotEmpty(statusDetails)) {
            StatusDetails details = statusDetails.get(0);
            if (CustomResource.getCRDName(SparkApplication.class).equalsIgnoreCase(details.getKind() + details.getGroup()) && sparkApplication.getMetadata().getName().equalsIgnoreCase(details.getName())) {
                result = true;
            }
        }
        return result;
    }

    private void handleStatus(Watcher.Action action, SparkApplication resource) {
        LOG.info("action = " + action);
        if (resource.getStatus() != null) {
            LOG.info("resource = " + JSONUtil.toJsonStr(resource.getStatus()));
        }

        if (isStreaming && action.equals(Watcher.Action.MODIFIED)) {
            SparkApplicationStatus status = resource.getStatus();
            if (status != null) {
                ApplicationState applicationState = status.getApplicationState();
                if (applicationState.getState().equalsIgnoreCase("COMPLETED")) {

                    //todo 这里还可以判断是否需要删除已经完成的任务
                    flag.getAndSet(true);
                    //主动抛出来异常停止当前任务
                    throw new RuntimeException("sparkApplication completed");
                }
            }
        }
    }
}
