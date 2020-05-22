package org.apache.flink.runtime.cloudmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.nonha.standalone.StandaloneHaServices;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.jobmaster.ResourceManagerAddress;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderelection.StandaloneLeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.registration.RegistrationConnectionListener;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.CloudManagerRegistration;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.TaskExecutorRegistration;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.FencedRpcEndpoint;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceUtils;
import org.apache.flink.runtime.taskexecutor.*;
import org.apache.flink.runtime.taskexecutor.exceptions.RegistrationTimeoutException;
import org.apache.flink.util.FlinkException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class CloudManager extends FencedRpcEndpoint<CloudManagerId> implements CloudManagerGateway, LeaderContender {
	public static final String CLOUD_MANAGER_NAME = "cloudmanager";
	private final FatalErrorHandler fatalErrorHandler;
	private RpcService rpcService;
	private String cloudId;
	/** Unique id of the cloud manager. */
	private final ResourceID resourceId;

	/** Ongoing registration of TaskExecutors per resource ID. */
	private final Map<ResourceID, CompletableFuture<TaskExecutorGateway>> taskExecutorGatewayFutures;

	private final Map<ResourceID, TaskExecutorGateway> taskExecutors;
	private final Map<ResourceID, SlotReport> slotReports;
	private final HighAvailabilityServices haServices;
	private final LeaderRetrievalService resourceManagerLeaderRetriever;
	@Nullable
	private EstablishedResourceManagerConnection establishedResourceManagerConnection;

	@Nullable
	private CloudManagerToResourceManagerConnection resourceManagerConnection;
	private ResourceManagerAddress resourceManagerAddress;

	/** The cloud manager configuration. */
	private final CloudManagerConfiguration cloudManagerConfiguration;

	@Nullable
	private UUID currentRegistrationTimeoutId;


	public CloudManager(RpcService rpcService,
						ResourceID rid,
						FatalErrorHandler fatalErrorHandler,
						HighAvailabilityServices haServices,
						CloudManagerConfiguration cmc){
		super(rpcService, CloudManager.CLOUD_MANAGER_NAME, null);
		this.rpcService = rpcService;
		this.resourceId = rid;
		this.taskExecutors = new HashMap<>(8);
		this.slotReports = new HashMap<>(4);
		this.taskExecutorGatewayFutures = new HashMap<>(8);
		this.fatalErrorHandler = checkNotNull(fatalErrorHandler);
		this.haServices = checkNotNull(haServices);
		this.resourceManagerLeaderRetriever = haServices.getResourceManagerLeaderRetriever();
		this.cloudManagerConfiguration = cmc;
		this.cloudId = cmc.getCloudId();
	}

	@Override
	public void onStart() throws Exception {
		try {
			LeaderElectionService les = ((StandaloneHaServices) haServices).getCloudManagerLeaderElectionService();
			les.start(this);
			log.info("CloudManager({}) start running.", resourceId);
			resourceManagerLeaderRetriever.start(new ResourceManagerLeaderListener());
		} catch (Exception e) {
			final ResourceManagerException exception = new ResourceManagerException(String.format("Could not start the ResourceManager %s", getAddress()), e);
			onFatalError(exception);
			throw exception;
		}
	}

	protected void onFatalError(Throwable t) {
		try {
			log.error("Fatal error occurred in CloudManager.", t);
		} catch (Throwable ignored) {}

		// The fatal error handler implementation should make sure that this call is non-blocking
		fatalErrorHandler.onFatalError(t);
	}

	@Override
	public CompletableFuture<Acknowledge> submitTask(String tdd, int jobMasterId, String timeout) {
		log.info("Receive submit task({}) of job({})from.", tdd, tdd);
		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	@Override
	public CompletableFuture<Acknowledge> cancelTask(ExecutionAttemptID executionAttemptID, Time timeout) {
		log.info("Receive task(ExecutionID: {}) cancel from jobManager.", executionAttemptID);
		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	@Override
	public CompletableFuture<Acknowledge> sendSlotReport(ResourceID taskManagerResourceId, SlotReport slotReport, Time timeout) {
		log.info("Receive slot report{} from TaskManager:{}.", slotReport, taskManagerResourceId);
		slotReports.put(taskManagerResourceId, slotReport);
		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	@Override
	public CompletableFuture<RegistrationResponse> registerTaskExecutor(TaskExecutorRegistration taskExecutorRegistration, Time timeout) {
		log.info("Receive TaskManager registration on deprecated method:{}.", taskExecutorRegistration.getResourceId());
		return null;
	}

	@Override
	public CompletableFuture<Acknowledge> requestSlot(SlotID slotId, JobID jobId, AllocationID allocationId, ResourceProfile resourceProfile, String targetAddress, ResourceManagerId resourceManagerId, Time timeout) {
		log.info("Receive slot({}) request(cloudid: {}, border: {}) of job({}) from resourcemanager in jobManager.",
			slotId,
			resourceProfile.getCloudId(),
			resourceProfile.isBorder(),
			jobId);
		CompletableFuture<Acknowledge> findResult = findAndRequestSlot(slotId,  jobId, allocationId, resourceProfile, targetAddress, resourceManagerId,  timeout);
		
		return findResult;
	}



	@Override
	public CompletableFuture<RegistrationResponse> registerTaskExecutorOnCloudManager(
		final TaskExecutorRegistration taskExecutorRegistration,
		final Time timeout) {
		log.info("Receive taskManager registration {}.", taskExecutorRegistration.getResourceId());
		CompletableFuture<TaskExecutorGateway> taskExecutorGatewayFuture = getRpcService().connect(taskExecutorRegistration.getTaskExecutorAddress(), TaskExecutorGateway.class);
		taskExecutorGatewayFutures.put(taskExecutorRegistration.getResourceId(), taskExecutorGatewayFuture);

		return taskExecutorGatewayFuture.handleAsync(
			(TaskExecutorGateway taskExecutorGateway, Throwable throwable) -> {
				final ResourceID resourceId = taskExecutorRegistration.getResourceId();
				if (taskExecutorGatewayFuture == taskExecutorGatewayFutures.get(resourceId)) {
					taskExecutorGatewayFutures.remove(resourceId);
					if (throwable != null) {
						log.info("Decline taskManager registration {}.", taskExecutorRegistration.getResourceId());
						return new RegistrationResponse.Decline(throwable.getMessage());
					} else {
						return registerTaskExecutorInternal(taskExecutorGateway, taskExecutorRegistration);
					}
				} else {
					log.info("Ignoring outdated TaskExecutorGateway connection.");
					return new RegistrationResponse.Decline("Decline outdated task executor registration.");
				}
			},
			getMainThreadExecutor());
	}

	/**
	 * Registers a new TaskExecutor.
	 *
	 * @param taskExecutorRegistration task executor registration parameters
	 * @return RegistrationResponse
	 */
	private RegistrationResponse registerTaskExecutorInternal(
		TaskExecutorGateway taskExecutorGateway,
		TaskExecutorRegistration taskExecutorRegistration) {
		ResourceID taskExecutorResourceId = taskExecutorRegistration.getResourceId();
		TaskExecutorGateway oldRegistration = taskExecutors.put(taskExecutorResourceId, taskExecutorGateway);
		if (oldRegistration != null) {
			// TODO :: suggest old taskExecutor to stop itself
			log.debug("Replacing old registration of TaskExecutor {}.", taskExecutorResourceId);
		}
		log.debug("Success registration of TaskExecutor {} on CloudManager.", taskExecutorResourceId);
		return new TaskExecutorRegistrationOnCloudManagerSuccess(resourceId);
	}


	private void notifyOfNewResourceManagerLeader(String newLeaderAddress, ResourceManagerId newResourceManagerId) {
		resourceManagerAddress = createResourceManagerAddress(newLeaderAddress, newResourceManagerId);
		reconnectToResourceManager(new FlinkException(String.format("ResourceManager leader changed to new address %s", resourceManagerAddress)));
	}

	private void reconnectToResourceManager(Exception cause) {
		closeResourceManagerConnection(cause);
		startRegistrationTimeout();
		tryConnectToResourceManager();
	}

	private CompletableFuture<Acknowledge> findAndRequestSlot(SlotID slotId, JobID jobId, AllocationID allocationId, ResourceProfile resourceProfile, String targetAddress, ResourceManagerId resourceManagerId, Time timeout) {
		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	private void closeResourceManagerConnection(Exception cause) {
		if (establishedResourceManagerConnection != null) {
			final ResourceID resourceManagerResourceId = establishedResourceManagerConnection.getResourceManagerResourceId();

			if (log.isDebugEnabled()) {
				log.debug("Close ResourceManager connection {}.",
					resourceManagerResourceId, cause);
			} else {
				log.info("Close ResourceManager connection {}.",
					resourceManagerResourceId);
			}

			ResourceManagerGateway resourceManagerGateway = establishedResourceManagerConnection.getResourceManagerGateway();
			resourceManagerGateway.disconnectCloudManager(getResourceID(), cause);

			establishedResourceManagerConnection = null;
		}

		if (resourceManagerConnection != null) {
			if (!resourceManagerConnection.isConnected()) {
				if (log.isDebugEnabled()) {
					log.debug("Terminating registration attempts towards ResourceManager {}.",
						resourceManagerConnection.getTargetAddress(), cause);
				} else {
					log.info("Terminating registration attempts towards ResourceManager {}.",
						resourceManagerConnection.getTargetAddress());
				}
			}

			resourceManagerConnection.close();
			resourceManagerConnection = null;
		}
	}


	private void startRegistrationTimeout() {
		final Time maxRegistrationDuration = cloudManagerConfiguration.getMaxRegistrationDuration();

		if (maxRegistrationDuration != null) {
			final UUID newRegistrationTimeoutId = UUID.randomUUID();
			currentRegistrationTimeoutId = newRegistrationTimeoutId;
			scheduleRunAsync(() -> registrationTimeout(newRegistrationTimeoutId), maxRegistrationDuration);
		}
	}

	private void stopRegistrationTimeout() {
		currentRegistrationTimeoutId = null;
	}

	private void registrationTimeout(@Nonnull UUID registrationTimeoutId) {
		if (registrationTimeoutId.equals(currentRegistrationTimeoutId)) {
			final Time maxRegistrationDuration = cloudManagerConfiguration.getMaxRegistrationDuration();

			onFatalError(
				new RegistrationTimeoutException(
					String.format("Could not register at the ResourceManager within the specified maximum " +
							"registration duration %s. This indicates a problem with this instance. Terminating now.",
						maxRegistrationDuration)));
		}
	}

	public ResourceID getResourceID() {
		return resourceId;
	}
	private void tryConnectToResourceManager() {
		if (resourceManagerAddress != null) {
			connectToResourceManager();
		}
	}

	private void connectToResourceManager() {
		assert(resourceManagerAddress != null);
		assert(establishedResourceManagerConnection == null);
		assert(resourceManagerConnection == null);

		log.info("Connecting to ResourceManager {}.", resourceManagerAddress);

		final CloudManagerRegistration cloudManagerRegistration = new CloudManagerRegistration(
			getAddress(),
			getResourceID(),
			cloudManagerConfiguration.getCloudId()
		);

		resourceManagerConnection =  new CloudManagerToResourceManagerConnection(
			log,
			getRpcService(),
			cloudManagerConfiguration.getRetryingRegistrationConfiguration(),
			resourceManagerAddress.getAddress(),
			resourceManagerAddress.getResourceManagerId(),
			getMainThreadExecutor(),
			new ResourceManagerRegistrationListener(),
			cloudManagerRegistration
		);

		resourceManagerConnection.start();
	}

	private ResourceManagerAddress createResourceManagerAddress(@Nullable String newLeaderAddress, @Nullable ResourceManagerId newResourceManagerId) {
		if (newLeaderAddress == null) {
			return null;
		} else {
			assert(newResourceManagerId != null);
			return new ResourceManagerAddress(newLeaderAddress, newResourceManagerId);
		}
	}

	@Override
	public void grantLeadership(UUID leaderSessionID) {
		final CloudManagerId id = CloudManagerId.fromUuid(leaderSessionID);
		if(getFencingToken() != null) {
			log.info("Fencing token already set to {}", getFencingToken());
		}
		setFencingToken(id);
	}

	@Override
	public void revokeLeadership() {
		runAsyncWithoutFencing(
			() -> {
				log.info("ResourceManager {} was revoked leadership. Clearing fencing token.", getAddress());

//				clearStateInternal();

				setFencingToken(null);

//				slotManager.suspend();

//				stopHeartbeatServices();

			});
	}

	@Override
	public void handleError(Exception exception) {
		log.info("Error in leader thread happend.");
	}

	private final class ResourceManagerLeaderListener implements LeaderRetrievalListener {

		@Override
		public void notifyLeaderAddress(final String leaderAddress, final UUID leaderSessionID) {
			runAsync(
				() -> notifyOfNewResourceManagerLeader(
					leaderAddress,
					ResourceManagerId.fromUuidOrNull(leaderSessionID)));
		}

		@Override
		public void handleError(Exception exception) {
			onFatalError(exception);
		}
	}

	private final class ResourceManagerRegistrationListener implements RegistrationConnectionListener<CloudManagerToResourceManagerConnection, CloudManagerRegistrationSuccess> {

		@Override
		public void onRegistrationSuccess(CloudManagerToResourceManagerConnection connection, CloudManagerRegistrationSuccess success) {
			final ResourceID resourceManagerId = success.getResourceManagerId();
			final InstanceID cloudManagerRegistrationId = success.getRegistrationId();
			final ClusterInformation clusterInformation = success.getClusterInformation();
			final ResourceManagerGateway resourceManagerGateway = connection.getTargetGateway();
			log.info("CloudManager({}) to JobManager registration success. Connection established.", resourceId);
			runAsync(
				() -> {
					// filter out outdated connections
					//noinspection ObjectEquality
					if (resourceManagerConnection == connection) {
						establishResourceManagerConnection(
							resourceManagerGateway,
							resourceManagerId,
							cloudManagerRegistrationId,
							clusterInformation);
					}
				});
		}


		@Override
		public void onRegistrationFailure(Throwable failure) {
			onFatalError(failure);
		}
	}


	private void establishResourceManagerConnection(ResourceManagerGateway resourceManagerGateway, ResourceID resourceManagerId, InstanceID taskExecutorRegistrationId, ClusterInformation clusterInformation) {
		establishedResourceManagerConnection = new EstablishedResourceManagerConnection(
			resourceManagerGateway,
			resourceManagerId,
			taskExecutorRegistrationId);

		stopRegistrationTimeout();
	}

}
