/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.runtime.cloudmanager.CloudManagerGateway;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;

import javax.annotation.Nonnull;

/**
 * Container for the cloud manager connection instances used by the
 * {@link TaskExecutor}.
 */
class EstablishedCloudManagerConnection {

	@Nonnull
	private final CloudManagerGateway cloudManagerGateway;

	@Nonnull
	private final ResourceID cloudManagerResourceId;
//
//	@Nonnull
//	private final InstanceID taskExecutorRegistrationId;

	EstablishedCloudManagerConnection(@Nonnull CloudManagerGateway gateway, @Nonnull ResourceID rId) {
		this.cloudManagerGateway = gateway;
		this.cloudManagerResourceId = rId;
	}

	@Nonnull
	public CloudManagerGateway getResourceManagerGateway() {
		return cloudManagerGateway;
	}

	@Nonnull
	public ResourceID getResourceManagerResourceId() {
		return cloudManagerResourceId;
	}

}
