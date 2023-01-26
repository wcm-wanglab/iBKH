# iBKH Case Study
## Overview
TBD
## Python Dependencies
The codes mainly depends on the scientific stacks on the Python 3.7.
```
numpy
pandas
torch
sklearn
matplotlib
neo4j
statsmodels
```
Install from PyPi
```
pip3 install numpy
```
## Running Guide
Please refer to each of the Jupiter Notebook files for detailed code instructions. The runtime of all code will depend on the performance of the hardware device. 
## Neo4j Setup
Since some of the data needs to be obtained by communicating with Neo4j, please refer to the deployment instructions for Neo4j on the homepage to deploy iBKH data to Neo4j. And replace the 'uri' variable in the generate_network_triplets() function in the exploration_CC.py file with your own URL for Neo4j.
## UMLS API Setup
The mapping of entities in the code uses the UMLS API, so you will need to complete the UMLS registration and obtain the API key associated with your account to complete access to the UMLS API.
