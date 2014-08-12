package mesosphere.marathon.state

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream }
import javax.inject.Inject

import mesosphere.marathon.Protos.StorageVersion
import mesosphere.marathon.state.PathId._
import mesosphere.marathon.state.StorageVersions._
import mesosphere.marathon.tasks.{ TaskIdUtil, TaskTracker }
import mesosphere.marathon.tasks.TaskTracker.App
import mesosphere.marathon.{ MarathonConf, BuildInfo, StorageException }
import mesosphere.util.BackToTheFuture.futureToFutureOption
import mesosphere.util.ThreadPoolContext.context
import mesosphere.util.{ BackToTheFuture, Logging }
import org.apache.mesos.state.{ State, Variable }

import scala.concurrent.duration.Duration
import scala.concurrent.{ Await, Future }
import scala.util.{ Failure, Success, Try }

class Migration @Inject() (
    state: State,
    appRepo: AppRepository,
    groupRepo: GroupRepository,
    config: MarathonConf,
    taskIdUtil: TaskIdUtil,
    implicit val timeout: BackToTheFuture.Timeout = BackToTheFuture.Implicits.defaultTimeout) extends Logging {

  type MigrationAction = (StorageVersion, () => Future[Any])

  /**
    * All the migrations, that have to be applied.
    * They get applied after the master has been elected.
    */
  def migrations: List[MigrationAction] = List(
    StorageVersions(0, 5, 0) -> { () => changeApps(app => app.copy(id = app.id.toString.toLowerCase.replaceAll("_", "-").toRootPath)) },
    StorageVersions(0, 7, 0) -> { () =>
      {
        changeTasks(app => new App(app.appName.canonicalPath(), app.tasks, app.shutdown))
        changeApps(app => app.copy(id = app.id.canonicalPath()))
        putAppsIntoGroup()
      }
    }
  )

  def applyMigrationSteps(from: StorageVersion): Future[List[StorageVersion]] = {
    val result = migrations.filter(_._1 > from).sortBy(_._1).map {
      case (migrateVersion, change) =>
        log.info(s"Migration for storage: ${from.str} to current: ${current.str}: apply change for version: ${migrateVersion.str} ")
        change.apply().map(_ => migrateVersion)
    }
    Future.sequence(result)
  }

  def migrate(): StorageVersion = {
    val result = for {
      changes <- currentStorageVersion.flatMap(applyMigrationSteps)
      storedVersion <- storeCurrentVersion
    } yield storedVersion

    result.onComplete {
      case Success(version) => log.info(s"Migration successfully applied for version ${version.str}")
      case Failure(ex)      => log.error(s"Migration failed! $ex")
    }

    Await.result(result, Duration.Inf)
  }

  private val storageVersionName = "internal:storage:version"

  def currentStorageVersion: Future[StorageVersion] = {
    state.fetch(storageVersionName).map {
      case Some(variable) => Try(StorageVersion.parseFrom(variable.value())).getOrElse(StorageVersions.empty)
      case None           => throw new StorageException("Failed to read storage version")
    }
  }

  def storeCurrentVersion: Future[StorageVersion] = {
    state.fetch(storageVersionName) flatMap {
      case Some(variable) =>
        state.store(variable.mutate(StorageVersions.current.toByteArray)) map {
          case Some(newVar) => StorageVersion.parseFrom(newVar.value)
          case None         => throw new StorageException(s"Failed to store storage version")
        }
      case None => throw new StorageException("Failed to read storage version")
    }
  }

  // specific migration helper methods

  private def changeApps(fn: AppDefinition => AppDefinition): Future[Any] = {
    appRepo.apps().flatMap { apps =>
      val mappedApps = apps.map { app => appRepo.store(fn(app)) }
      Future.sequence(mappedApps)
    }
  }

  private def changeTasks(fn: App => App): Future[Any] = {
    val taskTracker = new TaskTracker(state, config, taskIdUtil)
    def fetchApp(appId: PathId): Option[App] = {
      val bytes = state.fetch("tasks:" + appId.safePath).get().value
      if (bytes.length > 0) {
        val source = new ObjectInputStream(new ByteArrayInputStream(bytes))
        val fetchedTasks = taskTracker.deserialize(appId, source)
        Some(new App(appId, fetchedTasks, false))
      }
      else None
    }
    def store(app: App): Future[Variable] = {
      val oldVar = state.fetch("tasks:" + app.appName.safePath).get()
      val bytes = new ByteArrayOutputStream()
      val output = new ObjectOutputStream(bytes)
      taskTracker.serialize(app.appName, app.tasks, output)
      val newVar = oldVar.mutate(bytes.toByteArray)
      BackToTheFuture.futureToFuture(state.store(newVar))
    }
    appRepo.allPathIds().flatMap { apps =>
      val res = apps.flatMap(fetchApp).map{ app => store(fn(app)) }
      Future.sequence(res)
    }
  }

  private def putAppsIntoGroup(): Future[Any] = {
    groupRepo.group("root").map(_.getOrElse(Group.empty)).map { group =>
      appRepo.apps().flatMap { apps =>
        val updatedGroup = apps.foldLeft(group) { (group, app) =>
          val updatedApp = app.copy(id = app.id.canonicalPath())
          group.updateApp(updatedApp.id, _ => updatedApp, Timestamp.now())
        }
        groupRepo.store("root", updatedGroup)
      }
    }
  }
}

object StorageVersions {
  val VersionRegex = """^(\d+)\.(\d+)\.(\d+).*""".r

  def apply(major: Int, minor: Int, patch: Int): StorageVersion = {
    StorageVersion
      .newBuilder()
      .setMajor(major)
      .setMinor(minor)
      .setPatch(patch)
      .build()
  }

  def current: StorageVersion = {
    BuildInfo.version match {
      case VersionRegex(major, minor, patch) =>
        StorageVersions(
          major.toInt,
          minor.toInt,
          patch.toInt
        )
    }
  }

  implicit class OrderedStorageVersion(val version: StorageVersion) extends AnyVal with Ordered[StorageVersion] {
    override def compare(that: StorageVersion): Int = {
      def by(left: Int, right: Int, fn: => Int): Int = if (left.compareTo(right) != 0) left.compareTo(right) else fn
      by(version.getMajor, that.getMajor, by(version.getMinor, that.getMinor, by(version.getPatch, that.getPatch, 0)))
    }

    def str: String = s"Version(${version.getMajor}, ${version.getMinor}, ${version.getPatch})"
  }

  def empty: StorageVersion = StorageVersions(0, 0, 0)
}