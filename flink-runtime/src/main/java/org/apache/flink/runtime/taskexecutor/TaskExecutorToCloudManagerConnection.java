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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.cloudmanager.CloudManagerGateway;
import org.apache.flink.runtime.cloudmanager.CloudManagerId;
import org.apache.flink.runtime.registration.*;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.TaskExecutorRegistration;
import org.apache.flink.runtime.rpc.RpcService;
import org.slf4j.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The connection between a TaskExecutor and the ResourceManager.
 */
public class TaskExecutorToCloudManagerConnection
		extends RegisteredRpcConnection<CloudManagerId, CloudManagerGateway, TaskExecutorRegistrationOnCloudManagerSuccess> {

	private final RpcService rpcService;

	private final RetryingRegistrationConfiguration retryingRegistrationConfiguration;

	private final RegistrationConnectionListener<TaskExecutorToCloudManagerConnection, TaskExecutorRegistrationOnCloudManagerSuccess> registrationListener;

	private final TaskExecutorRegistration taskExecutorRegistration;

	public TaskExecutorToCloudManagerConnection(
			Logger log,
			RpcService rpcService,
			RetryingRegistrationConfiguration retryingRegistrationConfiguration,
			String resourceManagerAddress,
			CloudManagerId cloudManagerId,
			Executor executor,
			RegistrationConnectionListener<TaskExecutorToCloudManagerConnection, TaskExecutorRegistrationOnCloudManagerSuccess> registrationListener,
			TaskExecutorRegistration taskExecutorRegistration) {

		super(log, resourceManagerAddress, cloudManagerId, executor);

		this.rpcService = checkNotNull(rpcService);
		this.retryingRegistrationConfiguration = checkNotNull(retryingRegistrationConfiguration);
		this.registrationListener = checkNotNull(registrationListener);
		this.taskExecutorRegistration = checkNotNull(taskExecutorRegistration);
	}

	@Override
	protected RetryingRegistration<CloudManagerId, CloudManagerGateway, TaskExecutorRegistrationOnCloudManagerSuccess> generateRegistration() {
		return new TaskExecutorToCloudManagerConnection.CloudManagerRegistration(
			log,
			rpcService,
			getTargetAddress(),
			getTargetLeaderId(),
			retryingRegistrationConfiguration,
			taskExecutorRegistration);
	}

	@Override
	protected void onRegistrationSuccess(TaskExecutorRegistrationOnCloudManagerSuccess success) {
		log.info("Successful registration at cloud manager {} under registration id {}.",
			getTargetAddress(), success.getCloudManagerId());

		registrationListener.onRegistrationSuccess(this, success);
	}

	@Override
	protected void onRegistrationFailure(Throwable failure) {
		log.info("Failed to register at cloud manager {}.", getTargetAddress(), failure);

		registrationListener.onRegistrationFailure(failure);
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private static class CloudManagerRegistration
			extends RetryingRegistration<CloudManagerId, CloudManagerGateway, TaskExecutorRegistrationOnCloudManagerSuccess> {

		private final TaskExecutorRegistration taskExecutorRegistration;
		private Logger logger;
		CloudManagerRegistration(
				Logger log,
				RpcService rpcService,
				String targetAddress,
				CloudManagerId cloudManagerId,
				RetryingRegistrationConfiguration retryingRegistrationConfiguration,
				TaskExecutorRegistration taskExecutorRegistration) {

			super(log, rpcService, "CloudManager", CloudManagerGateway.class, targetAddress, cloudManagerId, retryingRegistrationConfiguration);
			this.taskExecutorRegistration = taskExecutorRegistration;
			this.logger = log;
		}


		@Override
		protected CompletableFuture<RegistrationResponse> invokeRegistration(CloudManagerGateway gateway, CloudManagerId fencingToken, long timeoutMillis) throws Exception {
			Time timeout = Time.milliseconds(timeoutMillis);
			return gateway.registerTaskExecutorOnCloudManager(
				taskExecutorRegistration,
				timeout);
		}
	}
}
