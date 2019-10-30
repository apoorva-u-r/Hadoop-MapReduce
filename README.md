# Amedeo Baragiola - HW2 CS441
#### Politecnico di Milano
#### University of Illinois at Chicago

## Introduction

This homework consists in creating different simulations on multiple datacenters and analyze the results obtained; the simulations are focused on the map/reduce architecture.
However, the policy implemented can be extended to another types of scenario by making few adjustments.

The simulation code has been written in Scala and can be compiled using SBT.
Multiple classes on the Cloudsim framework have been modified and extended to provide extra functionality.

## Installation instructions
This section contains the instructions on how to run the simulations implemented as part of this homework, the recommended procedure is to use IntellJ IDEA with the Scala plugin installed.

1. Open IntellJ IDEA, a welcome screen will be shown, select "Check out from Version Control" and then "Git".
2. Enter the following URL and click "Clone": https://bitbucket.org/abarag4/amedeo_baragiola_hw2.git
3. When prompted confirm with "Yes"
4. The SBT import screen will appear, proceed with the default options and confirm with "OK"
5. Confirm overwriting with "Yes"
6. You may now go to src/main/scala/com.abarag4/ where you can find the main rap/reduce driver class: MapReduceDriver. A run configuration is automatically created when you click the green arrow next to the main method of the driver class.

Note: All the required dependencies have been included in the build.sbt and plugins.sbt files. In case dependencies can't be found, check

#### Alternative: SBT from CLI

If you donâ€™t want to use an IDE, you may run this project from the command line (CLI), proceed as follows:

1. Type: git clone https://bitbucket.org/abarag4/amedeo_baragiola_hw1.git
2. Before running the actual code, you may wish to run tests with â€œsbt clean compile testâ€
3. Run the code: sbt clean compile run

Note: When you run either of the "sbt clean compile *" commands, you will be prompted to make a choice, please read the following section for more details.