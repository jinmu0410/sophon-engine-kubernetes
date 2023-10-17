package com.jm.sophon.engine.kubernetes.spark.deployment;

import com.jm.sophon.engine.kubernetes.spark.deployment.core.SophonContext;
import com.jm.sophon.engine.kubernetes.spark.deployment.model.SparkConfig;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/10/7 11:43
 */
public class SparkSophonContext implements SophonContext {

    private SparkConfig sparkConfig;

    public void setSparkConfig(SparkConfig sparkConfig) {
        this.sparkConfig = sparkConfig;
    }

    public SparkConfig getSparkConfig() {
        return sparkConfig;
    }
}
