resolvers += Resolver.url("commbank-releases-ivy", new URL("http://commbank.artifactoryonline.com/commbank/ext-releases-local-ivy"))(Patterns("[organization]/[module]_[scalaVersion]_[sbtVersion]/[revision]/[artifact](-[classifier])-[revision].[ext]"))

resolvers += "Local Maven Repository" at Path.userHome.asFile.toURI.toURL + ".m2/repository"

val uniformVersion = "0.1.0-20140604023218-251a40c"

addSbtPlugin("au.com.cba.omnia" % "uniform-core" % uniformVersion)

addSbtPlugin("au.com.cba.omnia" % "uniform-dependency" % uniformVersion)

addSbtPlugin("au.com.cba.omnia" % "uniform-assembly" % uniformVersion)

//addSbtPlugin("au.com.cba.omnia" % "ops-logical-build" % "5.0.0-20140328105246")

//addSbtPlugin("au.com.cba.omnia" % "tooling-jil-plugin" % "0.0.1-20140520120812")
