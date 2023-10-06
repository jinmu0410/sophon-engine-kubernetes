/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.jm.sophon.engine.kubernetes.flink.operator.status;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.jm.sophon.engine.kubernetes.flink.operator.spec.FlinkDeploymentSpec;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;


/**
 * Status of the last reconcile step for the flink deployment.
 */
@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class FlinkDeploymentReconciliationStatus extends ReconciliationStatus<FlinkDeploymentSpec> {

    @Override
    public Class<FlinkDeploymentSpec> getSpecClass() {
        return FlinkDeploymentSpec.class;
    }
}
