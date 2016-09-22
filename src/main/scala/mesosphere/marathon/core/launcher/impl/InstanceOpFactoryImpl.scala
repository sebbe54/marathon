package mesosphere.marathon.core.launcher.impl

import mesosphere.marathon.MarathonConf
import mesosphere.marathon.core.base.Clock
import mesosphere.marathon.core.instance.update.InstanceUpdateOperation
import mesosphere.marathon.core.instance.Instance.InstanceState
import mesosphere.marathon.core.instance.{ Instance, InstanceStatus }
import mesosphere.marathon.core.launcher.{ InstanceOp, InstanceOpFactory }
import mesosphere.marathon.core.task.Task
import mesosphere.marathon.core.plugin.PluginManager
import mesosphere.marathon.core.pod.PodDefinition
import mesosphere.marathon.plugin.task.RunSpecTaskProcessor
import mesosphere.marathon.plugin.{ ApplicationSpec, PodSpec }
import mesosphere.marathon.state.{ AppDefinition, DiskSource, ResourceRole, RunSpec }
import mesosphere.mesos.ResourceMatcher.ResourceSelector
import mesosphere.mesos.{ PersistentVolumeMatcher, ResourceMatcher, TaskBuilder, TaskGroupBuilder }
import mesosphere.util.state.FrameworkId
import org.apache.mesos.Protos.{ TaskGroupInfo, TaskInfo }
import org.apache.mesos.{ Protos => Mesos }
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq
import scala.concurrent.duration._

class InstanceOpFactoryImpl(
  config: MarathonConf,
  clock: Clock,
  pluginManager: PluginManager = PluginManager.None)
    extends InstanceOpFactory {

  private[this] val log = LoggerFactory.getLogger(getClass)
  private[this] val taskOperationFactory = {
    val principalOpt = config.mesosAuthenticationPrincipal.get
    val roleOpt = config.mesosRole.get

    new InstanceOpFactoryHelper(principalOpt, roleOpt)
  }

  private[this] lazy val appTaskProc: RunSpecTaskProcessor = combine(
    pluginManager.plugins[RunSpecTaskProcessor].toVector)

  override def buildTaskOp(request: InstanceOpFactory.Request): Option[InstanceOp] = {
    log.debug("buildTaskOp")

    request.runSpec match {
      case app: AppDefinition =>
        if (request.isForResidentRunSpec) {
          inferForResidents(request)
        } else {
          inferNormalTaskOp(request)
        }
      case pod: PodDefinition =>
        inferPodInstanceOp(request, pod)
      case _ =>
        throw new IllegalArgumentException(s"unsupported runSpec object ${request.runSpec}")
    }
  }

  protected def inferPodInstanceOp(request: InstanceOpFactory.Request, pod: PodDefinition): Option[InstanceOp] = {
    val builderConfig = TaskGroupBuilder.BuilderConfig(
      config.defaultAcceptedResourceRolesSet,
      config.envVarsPrefix.get)

    TaskGroupBuilder.build(pod, request.offer, Instance.Id.forRunSpec, builderConfig)(request.instances.toVector).map {
      case (executorInfo, groupInfo, hostPorts, instanceId) =>
        val agentInfo = Instance.AgentInfo(request.offer)
        val since = clock.now()
        val instance = Instance(
          instanceId,
          agentInfo = agentInfo,
          state = InstanceState(InstanceStatus.Created, since, pod.version, healthy = None),
          tasksMap = groupInfo.getTasksList.asScala.map { taskInfo =>
            // TODO(jdef) no support for resident tasks inside pods for the MVP
            val task = Task.LaunchedEphemeral(
              taskId = Task.Id(taskInfo.getTaskId),
              agentInfo = agentInfo,
              runSpecVersion = pod.version,
              status = Task.Status(since, taskStatus = InstanceStatus.Created),
              hostPorts = Seq.empty // TODO(jdef) confirm that it is appropriate to NOT include host ports here
            )
            task.taskId -> task
          }(collection.breakOut)
        )
        taskOperationFactory.launchEphemeral(executorInfo, groupInfo, Instance.LaunchRequest(
          instance, hostPorts.flatten))
    }
  }

  private[this] def inferNormalTaskOp(request: InstanceOpFactory.Request): Option[InstanceOp] = {
    val InstanceOpFactory.Request(runSpec, offer, instances, _) = request

    new TaskBuilder(runSpec, Task.Id.forRunSpec, config, Some(appTaskProc)).
      buildIfMatches(offer, instances.values.toVector).map {
        case (taskInfo, ports) =>
          val task = Task.LaunchedEphemeral(
            taskId = Task.Id(taskInfo.getTaskId),
            agentInfo = Instance.AgentInfo(offer),
            runSpecVersion = runSpec.version,
            status = Task.Status(
              stagedAt = clock.now(),
              taskStatus = InstanceStatus.Created
            ),
            hostPorts = ports.flatten
          )

          taskOperationFactory.launchEphemeral(taskInfo, task)
      }
  }

  private[this] def inferForResidents(request: InstanceOpFactory.Request): Option[InstanceOp] = {
    val InstanceOpFactory.Request(runSpec, offer, instances, additionalLaunches) = request

    // TODO(jdef) pods should be supported some day

    val needToLaunch = additionalLaunches > 0 && request.hasWaitingReservations
    val needToReserve = request.numberOfWaitingReservations < additionalLaunches

    /* *
     * If an offer HAS reservations/volumes that match our run spec, handling these has precedence
     * If an offer NAS NO reservations/volumes that match our run spec, we can reserve if needed
     *
     * Scenario 1:
     *  We need to launch tasks and receive an offer that HAS matching reservations/volumes
     *  - check if we have a task that need those volumes
     *  - if we do: schedule a Launch TaskOp for the task
     *  - if we don't: skip for now
     *
     * Scenario 2:
     *  We ned to reserve resources and receive an offer that has matching resources
     *  - schedule a ReserveAndCreate TaskOp
     */

    def maybeLaunchOnReservation = if (needToLaunch) {
      val maybeVolumeMatch = PersistentVolumeMatcher.matchVolumes(offer, request.reserved)

      maybeVolumeMatch.flatMap { volumeMatch =>

        // we must not consider the volumeMatch's Reserved task because that would lead to a violation of constraints
        // by the Reserved task that we actually want to launch
        val instancesToConsiderForConstraints = instances.values.filter { inst =>
          inst.tasks.exists(_.taskId != volumeMatch.task.taskId)
        }.toVector

        // resources are reserved for this role, so we only consider those resources
        val rolesToConsider = config.mesosRole.get.toSet
        val reservationLabels = TaskLabels.labelsForTask(request.frameworkId, volumeMatch.task).labels
        val matchingReservedResourcesWithoutVolumes =
          ResourceMatcher.matchResources(
            offer, runSpec, instancesToConsiderForConstraints,
            ResourceSelector.reservedWithLabels(rolesToConsider, reservationLabels)
          )

        matchingReservedResourcesWithoutVolumes.flatMap { otherResourcesMatch =>
          launchOnReservation(runSpec, offer, volumeMatch.task,
            matchingReservedResourcesWithoutVolumes, maybeVolumeMatch)
        }
      }
    } else None

    def maybeReserveAndCreateVolumes = if (needToReserve) {
      val configuredRoles = if (runSpec.acceptedResourceRoles.isEmpty) {
        config.defaultAcceptedResourceRolesSet
      } else {
        runSpec.acceptedResourceRoles
      }
      // We can only reserve unreserved resources
      val rolesToConsider = Set(ResourceRole.Unreserved).intersect(configuredRoles)
      if (rolesToConsider.isEmpty) {
        log.warn(s"Will never match for ${runSpec.id}. The runSpec is not configured to accept unreserved resources.")
      }

      val matchingResourcesForReservation =
        ResourceMatcher.matchResources(offer, runSpec, instances.values.toVector, ResourceSelector.reservable)
      matchingResourcesForReservation.map { resourceMatch =>
        reserveAndCreateVolumes(request.frameworkId, runSpec, offer, resourceMatch)
      }
    } else None

    maybeLaunchOnReservation orElse maybeReserveAndCreateVolumes
  }

  private[this] def launchOnReservation(
    spec: RunSpec,
    offer: Mesos.Offer,
    task: Task.Reserved,
    resourceMatch: Option[ResourceMatcher.ResourceMatch],
    volumeMatch: Option[PersistentVolumeMatcher.VolumeMatch]): Option[InstanceOp] = {

    // create a TaskBuilder that used the id of the existing task as id for the created TaskInfo
    new TaskBuilder(spec, (_) => task.taskId, config, Some(appTaskProc)).build(offer, resourceMatch, volumeMatch) map {
      case (taskInfo, ports) =>
        log.info("xxxxx launchOnReservation: spec.version {}", spec.version)

        val stateOp = InstanceUpdateOperation.LaunchOnReservation(
          task.taskId.instanceId,
          runSpecVersion = spec.version,
          timestamp = clock.now(),
          status = Task.Status(
            stagedAt = clock.now(),
            taskStatus = InstanceStatus.Created
          ),
          hostPorts = ports.flatten)

        taskOperationFactory.launchOnReservation(taskInfo, stateOp, task)
    }
  }

  private[this] def reserveAndCreateVolumes(
    frameworkId: FrameworkId,
    RunSpec: RunSpec,
    offer: Mesos.Offer,
    resourceMatch: ResourceMatcher.ResourceMatch): InstanceOp = {

    val localVolumes: Iterable[(DiskSource, Task.LocalVolume)] =
      resourceMatch.localVolumes.map {
        case (source, volume) =>
          (source, Task.LocalVolume(Task.LocalVolumeId(RunSpec.id, volume), volume))
      }
    val persistentVolumeIds = localVolumes.map { case (_, localVolume) => localVolume.id }
    val now = clock.now()
    val timeout = Task.Reservation.Timeout(
      initiated = now,
      deadline = now + config.taskReservationTimeout().millis,
      reason = Task.Reservation.Timeout.Reason.ReservationTimeout
    )
    val task = Task.Reserved(
      taskId = Task.Id.forRunSpec(RunSpec.id),
      agentInfo = Instance.AgentInfo(offer),
      reservation = Task.Reservation(persistentVolumeIds, Task.Reservation.State.New(timeout = Some(timeout))),
      status = Task.Status(
        stagedAt = now,
        taskStatus = InstanceStatus.Reserved
      )
    )
    val stateOp = InstanceUpdateOperation.Reserve(task)
    taskOperationFactory.reserveAndCreateVolumes(frameworkId, stateOp, resourceMatch.resources, localVolumes)
  }

  def combine(processors: Seq[RunSpecTaskProcessor]): RunSpecTaskProcessor = new RunSpecTaskProcessor {
    override def taskInfo(runSpec: ApplicationSpec, builder: TaskInfo.Builder): Unit = {
      processors.foreach(_.taskInfo(runSpec, builder))
    }
    override def taskGroup(runSpec: PodSpec, builder: TaskGroupInfo.Builder): Unit = {
      processors.foreach(_.taskGroup(runSpec, builder))
    }
  }
}
