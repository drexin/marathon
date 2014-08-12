package mesosphere.marathon.tasks

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream }

import com.codahale.metrics.MetricRegistry
import com.google.common.collect.Lists
import mesosphere.marathon.Protos.MarathonTask
import mesosphere.marathon.state.PathId
import mesosphere.marathon.state.PathId.StringPathId
import mesosphere.marathon.{ MarathonConf, MarathonSpec }
import mesosphere.mesos.protos.Implicits._
import mesosphere.mesos.protos.TextAttribute
import org.apache.mesos.Protos.{ TaskID, TaskState, TaskStatus }
import org.apache.mesos.state.{ InMemoryState, State }

import scala.collection.JavaConverters._
import scala.collection._
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class TaskTrackerTest extends MarathonSpec {

  val TEST_APP_NAME = "foo".toRootPath
  val TEST_TASK_ID = "sampleTask"
  var taskTracker: TaskTracker = null
  var state: State = null
  val config = mock[MarathonConf]
  val taskIdUtil = new TaskIdUtil

  before {
    val metricRegistry = new MetricRegistry
    state = new InMemoryState
    taskTracker = new TaskTracker(state, config, new TaskIdUtil)
  }

  def makeSampleTask(id: String) = {
    makeTask(id, "host", 999)
  }

  def makeTask(id: String, host: String, port: Int) = {
    MarathonTask.newBuilder()
      .setHost(host)
      .addAllPorts(Lists.newArrayList(port))
      .setId(id)
      .addAttributes(TextAttribute("attr1", "bar"))
      .build()
  }

  def makeTaskStatus(id: String, state: TaskState = TaskState.TASK_RUNNING) = {
    TaskStatus.newBuilder
      .setTaskId(TaskID.newBuilder
        .setValue(id)
      )
      .setState(state)
      .build
  }

  def shouldContainTask(tasks: Iterable[MarathonTask], task: MarathonTask) {
    assert(
      tasks.exists(t => t.getId == task.getId
        && t.getHost == task.getHost
        && t.getPortsList == task.getPortsList),
      s"Should contain task ${task.getId}"
    )
  }

  def shouldContainTaskStatus(task: MarathonTask, taskStatus: TaskStatus) {
    assert(
      task.getStatusesList.contains(taskStatus), s"Should contain task status ${taskStatus.getState.toString}"
    )
  }

  def stateShouldNotContainKey(state: State, key: String) {
    assert(!state.names().get().asScala.toSet.contains(key), s"Key ${key} was found in state")
  }

  test("SerializeAndDeserialize") {
    val sampleTask = makeSampleTask(TEST_TASK_ID)
    val byteOutputStream = new ByteArrayOutputStream()
    val outputStream = new ObjectOutputStream(byteOutputStream)
    val appId = PathId("/test")

    taskTracker.serialize(appId, Set(sampleTask), outputStream)

    val byteInputStream = new ByteArrayInputStream(byteOutputStream.toByteArray)
    val inputStream = new ObjectInputStream(byteInputStream)

    val deserializedTask = taskTracker.deserialize(appId, inputStream)

    assert(deserializedTask.equals(Set(sampleTask)), "Tasks are not properly serialized")
  }

  test("FetchApp") {
    val taskId1 = taskIdUtil.taskId(TEST_APP_NAME)
    val taskId2 = taskIdUtil.taskId(TEST_APP_NAME)
    val taskId3 = taskIdUtil.taskId(TEST_APP_NAME)

    val task1 = makeSampleTask(taskId1)
    val task2 = makeSampleTask(taskId2)
    val task3 = makeSampleTask(taskId3)

    taskTracker.created(TEST_APP_NAME, task1)
    taskTracker.created(TEST_APP_NAME, task2)
    taskTracker.created(TEST_APP_NAME, task3)

    taskTracker.store(TEST_APP_NAME)

    val testAppTasks = taskTracker.fetchApp(TEST_APP_NAME).tasks

    shouldContainTask(testAppTasks, task1)
    shouldContainTask(testAppTasks, task2)
    shouldContainTask(testAppTasks, task3)
    assert(testAppTasks.size == 3)
  }

  test("TaskLifecycle") {
    val sampleTask = makeSampleTask(TEST_TASK_ID)

    // CREATE TASK
    taskTracker.created(TEST_APP_NAME, sampleTask)

    shouldContainTask(taskTracker.get(TEST_APP_NAME), sampleTask)
    stateShouldNotContainKey(state, TEST_TASK_ID)

    // TASK STATUS UPDATE
    val startingTaskStatus = makeTaskStatus(TEST_TASK_ID, TaskState.TASK_STARTING)

    taskTracker.statusUpdate(TEST_APP_NAME, startingTaskStatus)

    shouldContainTask(taskTracker.get(TEST_APP_NAME), sampleTask)
    taskTracker.get(TEST_APP_NAME).foreach(task => shouldContainTaskStatus(task, startingTaskStatus))

    // TASK RUNNING
    val runningTaskStatus: TaskStatus = makeTaskStatus(TEST_TASK_ID, TaskState.TASK_RUNNING)

    taskTracker.running(TEST_APP_NAME, runningTaskStatus)

    shouldContainTask(taskTracker.get(TEST_APP_NAME), sampleTask)
    taskTracker.get(TEST_APP_NAME).foreach(task => shouldContainTaskStatus(task, runningTaskStatus))

    // TASK TERMINATED
    val finishedTaskStatus = makeTaskStatus(TEST_TASK_ID, TaskState.TASK_FINISHED)

    taskTracker.terminated(TEST_APP_NAME, finishedTaskStatus)

    assert(taskTracker.contains(TEST_APP_NAME), "App was not stored")
    stateShouldNotContainKey(state, TEST_TASK_ID)

    // APP SHUTDOWN
    taskTracker.shutdown(TEST_APP_NAME)

    assert(!taskTracker.contains(TEST_APP_NAME), "App was not removed")

    // ERRONEOUS MESSAGE
    val erroneousStatus = makeTaskStatus(TEST_TASK_ID, TaskState.TASK_LOST)

    val updatedTask = taskTracker.statusUpdate(TEST_APP_NAME, erroneousStatus)

    val taskOption = Await.result(updatedTask, Duration.Inf)

    // Empty option means this message was discarded since there was no matching task
    assert(taskOption.isEmpty, "Task was able to be updated and was not removed")
  }

  test("MultipleApps") {
    val appName1 = "app1".toRootPath
    val appName2 = "app2".toRootPath
    val appName3 = "app3".toRootPath

    val taskId1 = taskIdUtil.taskId(appName1)
    val taskId2 = taskIdUtil.taskId(appName1)
    val taskId3 = taskIdUtil.taskId(appName2)
    val taskId4 = taskIdUtil.taskId(appName3)
    val taskId5 = taskIdUtil.taskId(appName3)
    val taskId6 = taskIdUtil.taskId(appName3)

    val task1 = makeSampleTask(taskId1)
    val task2 = makeSampleTask(taskId2)
    val task3 = makeSampleTask(taskId3)
    val task4 = makeSampleTask(taskId4)
    val task5 = makeSampleTask(taskId5)
    val task6 = makeSampleTask(taskId6)

    taskTracker.created(appName1, task1)
    taskTracker.running(appName1, makeTaskStatus(taskId1))

    taskTracker.created(appName1, task2)
    taskTracker.running(appName1, makeTaskStatus(taskId2))

    taskTracker.created(appName2, task3)
    taskTracker.running(appName2, makeTaskStatus(taskId3))

    taskTracker.created(appName3, task4)
    taskTracker.running(appName3, makeTaskStatus(taskId4))

    taskTracker.created(appName3, task5)
    taskTracker.running(appName3, makeTaskStatus(taskId5))

    taskTracker.created(appName3, task6)
    taskTracker.running(appName3, makeTaskStatus(taskId6))

    assert(state.names.get.asScala.toSet.size == 3, "Incorrect number of tasks in state")

    val app1Tasks = taskTracker.fetchApp(appName1).tasks

    shouldContainTask(app1Tasks, task1)
    shouldContainTask(app1Tasks, task2)
    assert(app1Tasks.size == 2, "Incorrect number of tasks")

    val app2Tasks = taskTracker.fetchApp(appName2).tasks

    shouldContainTask(app2Tasks, task3)
    assert(app2Tasks.size == 1, "Incorrect number of tasks")

    val app3Tasks = taskTracker.fetchApp(appName3).tasks

    shouldContainTask(app3Tasks, task4)
    shouldContainTask(app3Tasks, task5)
    shouldContainTask(app3Tasks, task6)
    assert(app3Tasks.size == 3, "Incorrect number of tasks")
  }
}
