package org.apache.flink.runtime.cloudmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.TaskExecutorRegistration;
import org.apache.flink.runtime.rpc.FencedRpcGateway;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcTimeout;
import org.apache.flink.runtime.taskexecutor.SlotReport;

import java.util.concurrent.CompletableFuture;

public interface CloudManagerGateway extends FencedRpcGateway<CloudManagerId> {

	CompletableFuture<Acknowledge> submitTask(
		JobID id,
		String tname,
		int jobMasterId,
		String targetAddress);

	CompletableFuture<Acknowledge> cancelTask(
		ExecutionAttemptID executionAttemptID,
		Time timeout);

	CompletableFuture<Acknowledge> sendSlotReport(
		ResourceID taskManagerResourceId,
		SlotReport slotReport,
		@RpcTimeout Time timeout);

	CompletableFuture<RegistrationResponse> registerTaskExecutor(
		final TaskExecutorRegistration taskExecutorRegistration,
		final Time timeout);

	CompletableFuture<Acknowledge> requestSlot(
		SlotID slotId,
		JobID jobId,
		AllocationID allocationId,
		ResourceProfile resourceProfile,
		String targetAddress,
		ResourceManagerId resourceManagerId,
		@RpcTimeout Time timeout);

	CompletableFuture<RegistrationResponse> registerTaskExecutorOnCloudManager(
		final TaskExecutorRegistration taskExecutorRegistration,
		final Time timeout);
}
