package mesosphere.marathon.integration.setup

import mesosphere.marathon.core.pod.{HostNetwork, MesosContainer, PodDefinition}
import mesosphere.marathon.integration.facades.MarathonFacade._
import mesosphere.marathon.integration.setup.ProcessKeeper.MesosConfig
import mesosphere.marathon.raml
import mesosphere.marathon.state.{AppDefinition, Container}
import org.scalactic.source.Position
import org.scalatest.{BeforeAndAfter, GivenWhenThen, Matchers, Tag}

/**
  * All integration tests that use the Mesos containerizer with Docker images should be marked with this tag.
  */
object MesosDockerIntegrationTag extends Tag("mesosphere.marathon.MesosIntegrationTest")

class MesosAppIntegrationTest
    extends IntegrationFunSuite
    with SingleMarathonIntegrationTest
    with Matchers
    with BeforeAndAfter
    with GivenWhenThen {

  override def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit pos: Position): Unit = {
    super.test(testName, MesosDockerIntegrationTag +: testTags: _*)(testFun)
  }

  // Configure Mesos to provide the Mesos containerizer with Docker image support.
  override def startMesos(): Unit = {
    ProcessKeeper.startMesosLocal(MesosConfig(
      port = config.mesosPort,
      launcher = "linux",
      containerizers = "mesos",
      isolation = Some("filesystem/linux,docker/runtime"),
      imageProviders = Some("docker")))
  }

  //clean up state before running the test case
  before(cleanUp())

  test("deploy a simple Docker app using the Mesos containerizer") {
    Given("a new Docker app")
    val app = AppDefinition(
      id = testBasePath / "mesosdockerapp",
      cmd = Some("sleep 600"),
      container = Some(Container.MesosDocker(image = "busybox")),
      resources = raml.Resources(cpus = 0.2, mem = 16.0),
      instances = 1
    )

    When("The app is deployed")
    val result = marathon.createAppV2(app)

    Then("The app is created")
    result.code should be(201) // Created
    extractDeploymentIds(result) should have size 1
    waitForEvent("deployment_success")
    waitForTasks(app.id, 1) // The app has really started
  }

  test("deploy a simple pod") {
    Given("a pod with a single task")
    val pod = PodDefinition(
      id = testBasePath / "pod",
      containers = scala.collection.immutable.Seq(
        MesosContainer(
          name = "task1",
          exec = Some(raml.MesosExec(raml.ShellCommand("sleep 1000"))),
          resources = raml.Resources(cpus = 0.1, mem = 32.0)
        )
      ),
      networks = scala.collection.immutable.Seq(HostNetwork),
      instances = 1
    )

    When("The pod is deployed")
    val result = marathon.createPodV2(pod)

    Then("The pod is created")
    result.code should be(201) // Created
    //extractDeploymentIds(result) should have size 1 // TODO(PODS)
    waitForEvent("deployment_success")
    //waitForTasks(pod.id, 1) // The pod has really started // TODO(PODS)
  }
}
