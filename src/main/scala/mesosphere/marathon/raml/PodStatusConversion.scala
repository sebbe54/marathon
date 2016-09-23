package mesosphere.marathon.raml

import java.time.OffsetDateTime

import mesosphere.marathon.core.instance.{ Instance, InstanceStatus }
import mesosphere.marathon.core.pod.{ MesosContainer, PodDefinition }
import mesosphere.marathon.core.task.Task
import mesosphere.marathon.state.RunSpec

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq

trait PodStatusConversion {

  import PodStatusConversion._

  implicit val taskToContainerStatus: Writes[(PodDefinition, Task), ContainerStatus] = Writes { src =>
    val (pod, task) = src
    val since = task.status.startedAt.getOrElse(task.status.stagedAt).toOffsetDateTime // TODO(jdef) inaccurate

    val maybeContainerName: Option[String] = task.taskId.containerName
    assume(maybeContainerName.nonEmpty, s"task id ${task.taskId} does not have a valid container name")

    val maybeContainerSpec: Option[MesosContainer] = maybeContainerName.flatMap(pod.container(_))

    // possible that a new pod spec might not have a container with a name that was used in an old pod spec?
    val endpointStatus = endpointStatuses(pod, maybeContainerSpec, task)

    // some other layer should provide termination history

    // if, for some very strange reason, we cannot determine the container name from the task ID then default to
    // the Mesos task ID itself
    val displayName = task.taskId.containerName.getOrElse(task.taskId.mesosTaskId.getValue)

    // TODO(jdef) message
    ContainerStatus(
      name = displayName,
      status = task.status.taskStatus.toMesosStateName,
      statusSince = since,
      containerId = task.launchedMesosId.map(_.getValue),
      endpoints = endpointStatus,
      conditions = Seq(maybeHealthCondition(task.status, maybeContainerSpec, endpointStatus, since)).flatten,
      lastUpdated = since, // TODO(jdef) pods fixme
      lastChanged = since // TODO(jdef) pods.fixme
    )
  }

  /**
    * generate a pod instance status RAML for some instance.
    */
  @throws[IllegalArgumentException]("if you provide a non-pod `spec`")
  implicit val podInstanceStatusRamlWriter: Writes[(RunSpec, Instance), PodInstanceStatus] = Writes { src =>

    val (spec, instance) = src

    val pod: PodDefinition = spec match {
      case x: PodDefinition => x
      case _ => throw new IllegalArgumentException(s"expected a pod spec instead of $spec")
    }

    assume(
      pod.id == instance.instanceId.runSpecId,
      s"pod id ${pod.id} should match spec id of the instance ${instance.instanceId.runSpecId}")

    val containerStatus: Seq[ContainerStatus] = instance.tasks.map(t => Raml.toRaml((pod, t)))(collection.breakOut)
    val (derivedStatus: PodInstanceState, message: Option[String]) = podInstanceState(
      instance.state.status, containerStatus)

    val networkStatus: Seq[NetworkStatus] = networkStatuses(instance.tasks.toVector)
    val resources: Option[Resources] = allocatedResources(pod, instance)

    // TODO(jdef) message, conditions: for example it would probably be nice to see a "healthy" condition here that
    // summarizes the conditions of the same name for each of the instance's containers.
    PodInstanceStatus(
      id = instance.instanceId.idString,
      status = derivedStatus,
      statusSince = instance.state.since.toOffsetDateTime,
      agentHostname = Some(instance.agentInfo.host),
      resources = resources,
      networks = networkStatus,
      containers = containerStatus,
      message = message,
      lastUpdated = instance.state.since.toOffsetDateTime, // TODO(jdef) pods we don't actually track lastUpdated yet
      lastChanged = instance.state.since.toOffsetDateTime
    )
  }

  // TODO: Consider using a view here (since we flatMap and groupBy)
  def networkStatuses(tasks: Seq[Task]): Seq[NetworkStatus] = tasks.flatMap { task =>
    task.mesosStatus.filter(_.hasContainerStatus).fold(Seq.empty[NetworkStatus]) { mesosStatus =>
      mesosStatus.getContainerStatus.getNetworkInfosList.asScala.map { networkInfo =>
        NetworkStatus(
          name = if (networkInfo.hasName) Some(networkInfo.getName) else None,
          addresses = networkInfo.getIpAddressesList.asScala
            .withFilter(_.hasIpAddress).map(_.getIpAddress)(collection.breakOut)
        )
      }(collection.breakOut)
    }
  }.groupBy(_.name).values.map { toMerge =>
    val networkStatus: NetworkStatus = toMerge.reduceLeft { (merged, single) =>
      merged.copy(addresses = merged.addresses ++ single.addresses)
    }
    networkStatus.copy(addresses = networkStatus.addresses.distinct)
  }(collection.breakOut)

  def healthCheckEndpoint(spec: MesosContainer): Option[String] = spec.healthCheck match {
    case Some(HealthCheck(Some(HttpHealthCheck(endpoint, _, _)), _, _, _, _, _, _, _)) => Some(endpoint)
    case Some(HealthCheck(_, Some(TcpHealthCheck(endpoint)), _, _, _, _, _, _)) => Some(endpoint)
    case _ => None // no health check endpoint for this spec; command line checks aren't wired to endpoints!
  }

  /**
    * check that task is running; if so, calculate health condition according to possible command-line health check
    * or else endpoint health checks.
    */
  def maybeHealthCondition(
    status: Task.Status,
    maybeContainerSpec: Option[MesosContainer],
    endpointStatuses: Seq[ContainerEndpointStatus],
    since: OffsetDateTime): Option[StatusCondition] =

    status.taskStatus match {
      case InstanceStatus.Created | InstanceStatus.Staging | InstanceStatus.Starting | InstanceStatus.Reserved =>
        // not useful to report health conditions for tasks that have never reached a running state
        None
      case _ =>
        val healthy: Option[(Boolean, String)] = maybeContainerSpec.flatMap { containerSpec =>
          val usingCommandHealthCheck: Boolean = containerSpec.healthCheck.exists(_.command.nonEmpty)
          if (usingCommandHealthCheck) {
            Some(status.healthy.fold(false -> HEALTH_UNREPORTED) { _ -> HEALTH_REPORTED })
          } else {
            val ep = healthCheckEndpoint(containerSpec)
            ep.map { endpointName =>
              val epHealthy: Option[Boolean] = endpointStatuses.find(_.name == endpointName).flatMap(_.healthy)
              // health check endpoint was specified, but if we don't have a value for health yet then generate a
              // meaningful reason code
              epHealthy.fold(false -> HEALTH_UNREPORTED) { _ -> HEALTH_REPORTED }
            }
          }
        }
        healthy.map { h =>
          StatusCondition(
            name = STATUS_CONDITION_HEALTHY,
            lastChanged = since,
            lastUpdated = since, // TODO(jdef) pods only changes are propagated, so this isn't right
            value = h._1.toString,
            reason = Some(h._2)
          )
        }
    }

  def endpointStatuses(
    pod: PodDefinition,
    maybeContainerSpec: Option[MesosContainer],
    task: Task): Seq[ContainerEndpointStatus] =

    maybeContainerSpec.flatMap { containerSpec =>
      task.launched.flatMap { launched =>

        val taskHealthy: Option[Boolean] = // only calculate this once so we do it here
          launched.status.healthy

        task.taskId.containerName.flatMap { containerName =>
          pod.container(containerName).flatMap { containerSpec =>
            val endpointRequestedHostPort: Seq[String] =
              containerSpec.endpoints.withFilter(_.hostPort.isDefined).map(_.name)
            val reservedHostPorts: Seq[Int] = launched.hostPorts

            assume(
              endpointRequestedHostPort.size == reservedHostPorts.size,
              s"number of reserved host ports ${reservedHostPorts.size} should equal number of" +
                s"requested host ports ${endpointRequestedHostPort.size}")

            // we assume that order has been preserved between the allocated port list and the endpoint list
            // TODO(jdef) pods what actually guarantees that this doesn't change? (do we check this upon pod update?)
            def reservedEndpointStatus: Seq[ContainerEndpointStatus] =
              endpointRequestedHostPort.zip(reservedHostPorts).map {
                case (name, allocated) =>
                  ContainerEndpointStatus(name, Some(allocated))
              }

            def unreservedEndpointStatus: Seq[ContainerEndpointStatus] = containerSpec.endpoints
              .withFilter(_.hostPort.isEmpty).map(ep => ContainerEndpointStatus(ep.name))

            def withHealth: Seq[ContainerEndpointStatus] = {
              val allEndpoints = reservedEndpointStatus ++ unreservedEndpointStatus

              // check whether health checks are enabled for this endpoint. if they are then propagate the mesos task
              // health check result.
              healthCheckEndpoint(containerSpec).flatMap { name =>
                // update the `health` field of the endpoint status...
                allEndpoints.find(_.name == name).map(_.copy(healthy = taskHealthy))
              }.fold(allEndpoints) { updated =>
                // ... and replace the old entry with the one from above
                allEndpoints.filter(_.name != updated.name) ++ Seq(updated)
              }
            }
            Some(withHealth)
          }
        }
      }
    }.getOrElse(Seq.empty[ContainerEndpointStatus])

  def podInstanceState(
    status: InstanceStatus,
    containerStatus: Seq[ContainerStatus]): (PodInstanceState, Option[String]) = {

    import InstanceStatus._

    status match {
      case Created | Reserved =>
        PodInstanceState.Pending -> None
      case Staging | Starting =>
        PodInstanceState.Staging -> None
      case InstanceStatus.Error | Failed | Finished | Killed | Gone | Dropped | Unknown | Killing =>
        PodInstanceState.Terminal -> None
      case Unreachable =>
        PodInstanceState.Degraded -> Some(MSG_INSTANCE_UNREACHABLE)
      case Running =>
        if (containerStatus.exists(_.conditions.exists { cond =>
          cond.name == STATUS_CONDITION_HEALTHY && cond.value == "false"
        }))
          PodInstanceState.Degraded -> Some(MSG_INSTANCE_UNHEALTHY_CONTAINERS)
        else
          PodInstanceState.Stable -> None
    }
  }

  /**
    * @return the resources actually allocated/in-use by this pod instance; terminal instances don't count
    */
  def allocatedResources(pod: PodDefinition, instance: Instance): Option[Resources] = {
    import InstanceStatus._

    instance.state.status match {
      case Staging | Starting | Running | Reserved | Unreachable | Killing =>

        // Resources are automatically freed from the Mesos container as tasks transition to a terminal state.
        Some(pod.aggregateResources { ct =>
          instance.tasks.exists { task =>
            task.taskId.containerName == Some(ct.name) && (task.status.taskStatus match {
              case Staging | Starting | Running | Reserved | Unreachable | Killing => true
              case _ => false
            })
          }
        })

      case _ => None
    }
  }
}

object PodStatusConversion extends PodStatusConversion {

  val HEALTH_UNREPORTED = "health-unreported-by-mesos"
  val HEALTH_REPORTED = "health-reported-by-mesos"

  val STATUS_CONDITION_HEALTHY = "healthy"

  val MSG_INSTANCE_UNREACHABLE = "pod instance has become unreachable"
  val MSG_INSTANCE_UNHEALTHY_CONTAINERS = "at least one container is not healthy"
}
