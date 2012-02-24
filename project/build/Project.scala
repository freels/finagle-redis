import sbt._
import com.twitter.sbt._

class Project(info: ProjectInfo) extends StandardLibraryProject(info)
with DefaultRepos {

  //val finagle   = "com.twitter" % "finagle-core"      % "1.6.2"
  //val memcached = "com.twitter" % "finagle-memcached" % "1.6.2"

  // test jars

  val specs     = "org.scala-tools.testing" % "specs_2.8.1" % "1.6.6" % "test" withSources()
  val objenesis = "org.objenesis" % "objenesis"    % "1.1"    % "test"
  val jmock     = "org.jmock"     % "jmock"        % "2.4.0"  % "test"
  val hamcrest  = "org.hamcrest"  % "hamcrest-all" % "1.1"    % "test"
  val asm       = "asm"           % "asm"          % "1.5.3"  % "test"
  val cglib     = "cglib"         % "cglib"        % "2.2"    % "test"
}
