package com.jm.sophon.engine.kubernetes.spark.deployment.core;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/10/7 10:45
 */
public interface Deployment {

    void submit();

    void cancel();
}
