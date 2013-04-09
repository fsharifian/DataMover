#!/bin/sh
cd src
javac -cp ojdbc6.jar:ojdbc14.jar:com/rallydev/datamover:. com/rallydev/datamover/DataMover.java
