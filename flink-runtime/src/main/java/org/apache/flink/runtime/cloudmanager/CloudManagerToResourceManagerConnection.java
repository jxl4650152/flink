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

package org.apache.flink.runtime.cloudmanager;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.registration.*;
import org.apache.flink.runtime.resourcemanager.CloudManagerRegistration;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.TaskExecutorRegistration;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.taskexecutor.TaskExecutorRegistrationSuccess;
import org.slf4j.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The connection between a CloudManager and the ResourceManager.
 */
public class CloudManagerToResourceManagerConnection
		extends RegisteredRpcConnection<ResourceManagerId, ResourceManagerGateway, CloudManagerRegistrationSuccess> {

	private final RpcService rpcService;

	private final RetryingRegistrationConfiguration retryingRegistrationConfiguration;

	private final RegistrationConnectionListener<CloudManagerToResourceManagerConnection, CloudManagerRegistrationSuccess> registrationListener;

	private final CloudManagerRegistration cloudManagerRegistration;

	public CloudManagerToResourceManagerConnection(
			Logger log,
			RpcService rpcService,
			RetryingRegistrationConfiguration retryingRegistrationConfiguration,
			String resourceManagerAddress,
			ResourceManagerId resourceManagerId,
			Executor executor,
			RegistrationConnectionListener<CloudManagerToResourceManagerConnection, CloudManagerRegistrationSuccess> registrationListener,
			CloudManagerRegistration cloudManagerRegistration) {

		super(log, resourceManagerAddress, resourceManagerId, executor);

		this.rpcService = checkNotNull(rpcService);
		this.retryingRegistrationConfiguration = checkNotNull(retryingRegistrationConfiguration);
		this.registrationListener = checkNotNull(registrationListener);
		this.cloudManagerRegistration = checkNotNull(cloudManagerRegistration);
	}

	public String getCloudId(){
		return cloudManagerRegistration.getCloudId();
	}

	@Override
	protected RetryingRegistration<ResourceManagerId, ResourceManagerGateway, CloudManagerRegistrationSuccess> generateRegistration() {
		return new CloudManagerToResourceManagerConnection.ResourceManagerRegistration(
			log,
			rpcService,
			getTargetAddress(),
			getTargetLeaderId(),
			retryingRegistrationConfiguration,
			cloudManagerRegistration);
	}

	@Override
	protected void onRegistrationSuccess(CloudManagerRegistrationSuccess success) {
		log.info("Successful registration at resource manager {} under registration id {}.",
			getTargetAddress(), success.getRegistrationId());

		registrationListener.onRegistrationSuccess(this, success);
	}

	@Override
	protected void onRegistrationFailure(Throwable failure) {
		log.info("Failed to register at resource manager {}.", getTargetAddress(), failure);

		registrationListener.onRegistrationFailure(failure);
	}


	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private static class ResourceManagerRegistration
			extends RetryingRegistration<ResourceManagerId, ResourceManagerGateway, CloudManagerRegistrationSuccess> {

		private final CloudManagerRegistration cloudManagerRegistration;

		ResourceManagerRegistration(
				Logger log,
				RpcService rpcService,
				String targetAddress,
				ResourceManagerId resourceManagerId,
				RetryingRegistrationConfiguration retryingRegistrationConfiguration,
				CloudManagerRegistration cloudManagerRegistration) {

			super(log, rpcService, "ResourceManager", ResourceManagerGateway.class, targetAddress, resourceManagerId, retryingRegistrationConfiguration);
			this.cloudManagerRegistration = cloudManagerRegistration;
		}

		@Override
		protected CompletableFuture<RegistrationResponse> invokeRegistration(
				ResourceManagerGateway resourceManager, ResourceManagerId fencingToken, long timeoutMillis) throws Exception {

			Time timeout = Time.milliseconds(timeoutMillis);
			return resourceManager.registerCloudManager(
				cloudManagerRegistration,
				timeout);
		}
	}
}
