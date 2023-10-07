package com.jm.sophon.engine.kubernetes.spark.deployment.core;

import com.jm.sophon.engine.kubernetes.spark.deployment.model.SparkConfig;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/10/7 10:48
 */
public abstract class AbstractClusterDeployment<T extends SophonContext> implements Deployment{

    protected SparkConfig sparkConfig;

    protected T t;

    public AbstractClusterDeployment(T t){
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

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            post();
        }
    }
}
