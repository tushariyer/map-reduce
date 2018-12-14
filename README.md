# Using Map-Reduce to calculate Diversity Index
## DivIndex by Tushar Iyer

The United States Census Bureau (USCB) estimates the number of people in each county in each state. These estimates are categorized by gender, age, race, and other factors.

The **diversity index**  _D_ for a population is the probability that two random people from a given population will be of different races. The diversity index is calculated with the following formula, where _N_<sub>I</sub> is the number of individuals in racial category _i_ and _T_ is the total number of individuals:

$D = \frac{1}{T^2}\sum_{i=1}^{6}N_i(T - N_i)$

This project works with the census dataset sourced [here](https://www.census.gov/data/tables/2017/demo/popest/counties-detail.html).

The program was tested on a multicore cluster computer at RIT, but can be used with other cluster machine that have [Parallel Java 2](https://www.cs.rit.edu/~ark/pj2.shtml) installed. Parallel Java 2 was developed by Alan Kaminsky in the Department of Computer Science at the Rochester Institute of Technology. The link includes a description of PJ2 and its installation guide. Documentation for PJ2 can be found on the same webpage. PJ2 is distributed under the terms of the GNU General Public License as published by the Free Software Foundation.


## Compilation

The project comes with the three `.java` files necessary for this program and can be compiled on a machine with PJ2 installed using the following steps:

 - Navigate to the directory where the `.java` source files are located
 - Export  JDK 1.7 classpath  `export PATH=/usr/local/dcs/versions/jdk1.7.0_51/bin:$PATH`
 - Include PJ2 in classpath `export CLASSPATH=.:/var/tmp/parajava/pj2/pj2.jar`
 - Make `build` directory with `mkdir build`
 - Compile source code with `javac -d ./build *.java`
 - Enter the `build` directory: `cd build`
 - Build `jar` with `jar cvf <name>.jar *` where `<name>` is what you want to call the `jar`

Now assuming the machine has PJ2's tracker set up correctly, the names of all nodes are known and the census dataset has been downloaded and split properly amongst all nodes, you are ready to run the program.

## Execution

Programs written with/for PJ2 are run by using PJ2 as a launcher, so it is imperative to get the command line arguments right. This program `DivIndex` is launched with the following parameters:

`java pj2 debug=<debug> timelimit=<s> jar=<name>.jar threads=<thr> DivIndex <nodes> <path/to/dataset> <year> <states>`
 - `<debug>` is a parameter set to `none` if no job-specific information is to be printed out or `makespan` if you want to see job-related information and running times.
 - `<s>` is the number of seconds you want to allow the program to run for before timing out
 - `<thr>` is the number of threads you want to devote to this task. Defaults to `1` if omitted.
 - `<nodes>` is all nodes to be used, separated by commas
 - `<path/to/dataset>` is the relative path to the csv file where the census dataset is partitioned on each node
 - `<year>` is an integer argument from `1` to `10`, with `1` referring to 2007, and 	`10` referring to 2017
 - `<states>` is an optional argument. If none are provided, the program will calculate the diversity index for every county in all 50 states as well as the District of Columbia. Else, the states should be passed in as quoted strings, delimited by a single space.

The program will print output such that the states are in alphabetical order, but the counties will be listed in descending order of diversity index.

## Screenshots

Below are three screenshots of the program running with different sets of parameters:
