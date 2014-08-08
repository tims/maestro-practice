#!/bin/sh
mvn install:install-file -DgroupId=com.teradata -DartifactId=terajdbc4 -Dversion=15.00.00.15 -Dpackaging=jar -Dfile=terajdbc4-15.00.00.15.jar -DgeneratePom=true
mvn install:install-file -DgroupId=com.teradata -DartifactId=tdgssconfig -Dversion=15.00.00.15 -Dpackaging=jar -Dfile=tdgssconfig-15.00.00.15.jar -DgeneratePom=true
