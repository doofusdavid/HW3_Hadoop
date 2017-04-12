# Overview

FYI: Per Piazza, all analyses will disregard VI and PR, as the numbers are not correct.

### Code overview
The code matures over time as I figured out better ways to do things.  Rather than string packing and unpacking, by Q3 I was 
using a data class that implemented the Writable interface to store data.  This had much better results.

My Job classes generally would take the output file, reparse it, and put out data in the format desired.  My string
formatting skills improved over time, such that the first few questions are a bit hard to read the results of.

Even with the subset of census data available to us, I still found that running these tasks locally resulted in a considerable
 speed increase, which was fairly critical as I couldn't really step through to debug the code.  As a result, I used
 [Docker](https://docker.com), running a [Hadoop Container](https://hub.docker.com/r/izone/hadoop/).  I needed to change my JVM to 
 match theirs, but after that, I was able to copy the provided sample census data into hdfs on the docker instance, and
 run my tests directly on my local machine.  This GREATLY sped up the development process.
 
 After confirming that they were working, I would copy my jar file to the CS machines, and run against the entire census
 set.  Towards the end, I created a [batch file](../../runall.sh) to run all of the finalized jobs at once.  
 
 As a result, I modified my [build.xml](../../build.xml) file to add two new distributions.  One, would copy the jar file 
 to my shared docker folder, so I could run from inside that container.  Then, the other would copy to Honolulu (my CS machine
 of choice, since I was in Hawaii when this class started).  


### Analysis
* [Q1 Analysis](Q1.md)
* [Q2 Analysis](Q2.md)
* [Q3 Analysis](Q3.md)
* [Q4 Analysis](Q4.md)
* [Q5 Analysis](Q5.md)
* [Q6 Analysis](Q6.md)
* [Q7 Analysis](Q7.md)
* [Q8 Analysis](Q8.md)
* [Q9 Analysis](Q9.md)
