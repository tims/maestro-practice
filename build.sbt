uniform.project("scalding-teradata", "au.com.cba.omnia")

Defaults.defaultSettings

uniformDependencySettings

uniformAssemblySettings

libraryDependencies :=
  depend.scalding() ++ depend.hadoop() ++ depend.scalaz() ++ depend.scaldingproject() ++
  Seq(
    "au.com.cba.omnia" %% "omnia-test"        % "2.1.0-20140604032817-d3b19f6" % "test",
    "com.teradata"     % "terajdbc4"          % "15.00.00.15",
    "com.teradata"     % "tdgssconfig"        % "15.00.00.15",
    "com.twitter"      % "scalding-jdbc_2.10" % "0.10.0"
  ) ++ depend.omnia("thermometer", "0.3.0-20140725044031-74e7e94")  
