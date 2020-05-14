package org.apache.flink.runtime.cloudmanager;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.TaskExecutorRegistration;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcTimeout;
import org.apache.flink.runtime.taskexecutor.SlotReport;

import java.util.concurrent.CompletableFuture;

public interface CloudManagerGateway extends RpcGateway {

	CompletableFuture<Acknowledge> submitTask(
		String tdd,
		int jobMasterId,
		String timeout);

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


	CompletableFuture<RegistrationResponse> registerTaskExecutorOnCloudManager(
		final TaskExecutorRegistration taskExecutorRegistration,
		final Time timeout);
}
