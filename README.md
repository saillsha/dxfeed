## setting up Java env ##
* Download Eclipse
* ```git clone https://github.com/saillsha/dxfeed.git``` 
* Click "Create New Project" in Eclipse
  * Point the project location to be the root directory of the cloned repo.
  * Under "Project Layout", choose "Use Project as root for sources and class files"
* Check that you are using JRE 1.7 (Eclipse by default downloads JRE 1.6, which will not be able to run the dxfeed code). You will get a run-time error. The upgrade process varies by OS, so I suggest Googling to find the solution for you.
* Try running the FetchDailyCandles.java, by right clicking on the file -> Run As -> Run Configurations -> Arguments -> "IBM"

## settings up hadoop ##
* http://amodernstory.com/2014/09/23/installing-hadoop-on-mac-osx-yosemite/ <- that tutorial works most of the way, just make sure you change the directory path used in the hstart and hstop aliases to 2.7.0

## setting up kafka ##
* http://kafka.apache.org/documentation.html#quickstart
