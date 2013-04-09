#!/bin/sh
cd src
java -Xms8g -Xmx8g -XX:MaxPermSize=256m -cp ojdbc6.jar:com/rallydev/datamover:. com.rallydev.datamover.DataMover > datamover.log 2>&1 &
