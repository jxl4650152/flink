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
import org.apache.flink.configuration.*;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders;
import org.apache.flink.runtime.registration.RetryingRegistrationConfiguration;
import org.apache.flink.runtime.taskexecutor.TaskExecutor;
import org.apache.flink.runtime.taskexecutor.TaskExecutorResourceSpec;
import org.apache.flink.runtime.taskexecutor.TaskExecutorResourceUtils;
import org.apache.flink.runtime.taskmanager.TaskManagerRuntimeInfo;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.Duration;

/**
 * Configuration object for {@link CloudManager}.
 */
public class CloudManagerConfiguration {

	private static final Logger LOG = LoggerFactory.getLogger(CloudManagerConfiguration.class);


	private final Time timeout;

	// null indicates an infinite duration
	@Nullable
	private final Time maxRegistrationDuration;

	private final Time initialRegistrationPause;
	private final Time maxRegistrationPause;
	private final Time refusedRegistrationPause;
	private final String cloudId;

	private final RetryingRegistrationConfiguration retryingRegistrationConfiguration;

	public CloudManagerConfiguration(
			Time timeout,
			@Nullable Time maxRegistrationDuration,
			Time initialRegistrationPause,
			Time maxRegistrationPause,
			Time refusedRegistrationPause,
			RetryingRegistrationConfiguration retryingRegistrationConfiguration,
			String id) {

		this.timeout = Preconditions.checkNotNull(timeout);
		this.maxRegistrationDuration = maxRegistrationDuration;
		this.initialRegistrationPause = Preconditions.checkNotNull(initialRegistrationPause);
		this.maxRegistrationPause = Preconditions.checkNotNull(maxRegistrationPause);
		this.refusedRegistrationPause = Preconditions.checkNotNull(refusedRegistrationPause);
		this.retryingRegistrationConfiguration = retryingRegistrationConfiguration;
		this.cloudId = id;
	}




	public Time getTimeout() {
		return timeout;
	}

	@Nullable
	public Time getMaxRegistrationDuration() {
		return maxRegistrationDuration;
	}

	public Time getInitialRegistrationPause() {
		return initialRegistrationPause;
	}

	@Nullable
	public Time getMaxRegistrationPause() {
		return maxRegistrationPause;
	}

	public Time getRefusedRegistrationPause() {
		return refusedRegistrationPause;
	}


	public RetryingRegistrationConfiguration getRetryingRegistrationConfiguration() {
		return retryingRegistrationConfiguration;
	}

	public String getCloudId(){ return cloudId; }
	// --------------------------------------------------------------------------------------------
	//  Static factory methods
	// --------------------------------------------------------------------------------------------

	public static CloudManagerConfiguration fromConfiguration(
			Configuration configuration) {

		final Time timeout;
		try {
			timeout = AkkaUtils.getTimeoutAsTime(configuration);
		} catch (Exception e) {
			throw new IllegalArgumentException(
				"Invalid format for '" + AkkaOptions.ASK_TIMEOUT.key() +
					"'.Use formats like '50 s' or '1 min' to specify the timeout.");
		}

		LOG.info("Messages have a max timeout of " + timeout);

		Time finiteRegistrationDuration;
		try {
			Duration maxRegistrationDuration = configuration.get(TaskManagerOptions.REGISTRATION_TIMEOUT);
			finiteRegistrationDuration = Time.milliseconds(maxRegistrationDuration.toMillis());
		} catch (IllegalArgumentException e) {
			LOG.warn("Invalid format for parameter {}. Set the timeout to be infinite.",
				TaskManagerOptions.REGISTRATION_TIMEOUT.key());
			finiteRegistrationDuration = null;
		}

		final Time initialRegistrationPause;
		try {
			Duration pause = configuration.get(TaskManagerOptions.INITIAL_REGISTRATION_BACKOFF);
			initialRegistrationPause = Time.milliseconds(pause.toMillis());
		} catch (IllegalArgumentException e) {
			throw new IllegalArgumentException("Invalid format for parameter " +
				TaskManagerOptions.INITIAL_REGISTRATION_BACKOFF.key(), e);
		}

		final Time maxRegistrationPause;
		try {
			Duration pause = configuration.get(TaskManagerOptions.REGISTRATION_MAX_BACKOFF);
			maxRegistrationPause = Time.milliseconds(pause.toMillis());
		} catch (IllegalArgumentException e) {
			throw new IllegalArgumentException("Invalid format for parameter " +
				TaskManagerOptions.INITIAL_REGISTRATION_BACKOFF.key(), e);
		}

		final Time refusedRegistrationPause;
		try {
			Duration pause = configuration.get(TaskManagerOptions.REFUSED_REGISTRATION_BACKOFF);
			refusedRegistrationPause = Time.milliseconds(pause.toMillis());
		} catch (IllegalArgumentException e) {
			throw new IllegalArgumentException("Invalid format for parameter " +
				TaskManagerOptions.INITIAL_REGISTRATION_BACKOFF.key(), e);
		}

		final RetryingRegistrationConfiguration retryingRegistrationConfiguration = RetryingRegistrationConfiguration.fromConfiguration(configuration);

		String cloudId = configuration.get(CloudManagerOptions.CLOUD_ID);
		return new CloudManagerConfiguration(
			timeout,
			finiteRegistrationDuration,
			initialRegistrationPause,
			maxRegistrationPause,
			refusedRegistrationPause,
			retryingRegistrationConfiguration,
			cloudId);
	}
}
