package com.jm.sophon.engine.kubernetes.spark.deployment.core;

import cn.hutool.json.JSONUtil;
import com.jm.sophon.engine.kubernetes.spark.config.KubernetesClientAdapter;
import com.jm.sophon.engine.kubernetes.spark.deployment.model.SparkConfig;
import com.jm.sophon.engine.kubernetes.spark.operator.ApplicationState;
import com.jm.sophon.engine.kubernetes.spark.operator.SparkApplication;
import com.jm.sophon.engine.kubernetes.spark.operator.SparkApplicationStatus;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.FutureTask;
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

    protected void watch() {
        // 异步监听sparkApplication状态
        FutureTask processLogFuture = new FutureTask<>(this::watchStatus, null);
        Thread processLogThread = new Thread(processLogFuture, this.sparkApplication.getMetadata().getName() + "-watch-status");
        processLogThread.setDaemon(true);
        processLogThread.start();
    }

    public void watchStatus() {
        Watch watch = getSparkApplicationStatusWatch();
        while (true) {
            if (flag.get()) {
                LOG.info("sparkApplication watch completed");
                watch.close();
                break;
            }
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public Watch getSparkApplicationStatusWatch() {
        return this.kubernetesClientAdapter.getClient().resource(this.sparkApplication).inNamespace(this.sparkApplication.getMetadata().getNamespace())
                .watch(new Watcher<SparkApplication>() {
                    @Override
                    public void eventReceived(Action action, SparkApplication resource) {
                        //todo 暂时只做状态日志的打印，后期需要怎么利用状态，再处理
                        handleStatus(action, resource);
                    }

                    @Override
                    public void onClose(WatcherException cause) {
                        LOG.info("sparkApplicationStatusWatch onclose message = " + cause.getMessage());
                    }
                });
    }

    public void handleStatus(Watcher.Action action, SparkApplication resource) {
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
                    throw new RuntimeException("sparkApplication completed");
                }
            }
        }
    }
}
