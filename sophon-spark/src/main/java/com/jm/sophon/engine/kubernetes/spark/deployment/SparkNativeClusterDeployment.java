package com.jm.sophon.engine.kubernetes.spark.deployment;

import com.jm.sophon.engine.kubernetes.spark.deployment.core.AbstractNativeClusterDeployment;
import com.jm.sophon.engine.kubernetes.spark.deployment.core.SophonContext;

import java.util.List;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/10/9 19:59
 */
public class SparkNativeClusterDeployment extends AbstractNativeClusterDeployment {


    public SparkNativeClusterDeployment(SophonContext sophonContext) {
        super(sophonContext);
    }

    @Override
    protected List<String> buildCommand() {
        return null;
    }

    @Override
    public void pre() {

    }


    @Override
    public void post() {

    }

    @Override
    public void cancel() {

    }
}
