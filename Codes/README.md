# iBKH Case Study
## Overview
TBD

## Python Dependencies
The codes mainly depends on the scientific stacks on the Python 3.7.
```
numpy 1.21.5
pandas 1.3.5
torch 1.2.0 (https://pytorch.org/)
sklearn 0.0
neo4j 5.2.0 (https://pypi.org/project/neo4j/5.2.0/)
matplotlib 3.1.1
statsmodels 0.11.1
```

## DGL-KE Platform Setup

## Knowledge Graph Embedding

## Case Study I


## Case Study II
### Step I: All-of-Us

### Step II: xxx
Note1: Neo4j-Python Setup...
Note2: We used UMLS to,,,



## Running Guide
Please refer to each of the Jupiter Notebook files for detailed code instructions. The runtime of all code will depend on the performance of the hardware device. 
## Neo4j Setup
Since some of the data needs to be obtained by communicating with Neo4j, please refer to the deployment instructions for Neo4j on the homepage to deploy iBKH data to Neo4j. And replace the 'uri' variable in the generate_network_triplets() function in the exploration_CC.py file with your own URL for Neo4j.
## UMLS API Setup
The mapping of entities in the code uses the UMLS API, so you will need to complete the UMLS registration and obtain the API key associated with your account to complete access to the UMLS API.
