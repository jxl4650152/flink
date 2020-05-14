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

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.HardwareDescription;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Information provided by the CloudManager when it registers to the ResourceManager.
 */
public class CloudManagerRegistration implements Serializable {
	private static final long serialVersionUID = -5727832919954047964L;

	/**
	 * The address of the CloudManager that registers.
	 */
	private final String cloudManagerAddress;

	/**
	 * The resource ID of the CloudManager that registers.
	 */
	private final ResourceID resourceId;

	/**
	 * id of CloudManager.
	 */
	private final String cloudId;

	public CloudManagerRegistration(
			final String cloudManagerAddress,
			final ResourceID resourceId,
			final String cloudId) {
		this.cloudManagerAddress = checkNotNull(cloudManagerAddress);
		this.resourceId = checkNotNull(resourceId);
		this.cloudId = cloudId;
	}

	public String getCloudManagerAddress() {
		return cloudManagerAddress;
	}

	public ResourceID getResourceId() {
		return resourceId;
	}

	public String getCloudId() {
		return cloudId;
	}

}
