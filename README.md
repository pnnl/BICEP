# Behind-the-meter Infrastructure Costs for Electrification Progression (BICEP) model

This repo contains the code for the BICEP model. It is part of EERE's Decarbonizing Energy through Collaborative Analysis of Routes and Benefits (DECARB) Task 5: Estimating Electrical Distribution System and behind-the-meter (BTM) Costs

BICEP is a probabilistic model that provides a granular estimation of existing electrical capacity and the required additional capacity for the 
various decarbonization technologies in the DECARB scenarios based on the net change in load on a per-customer basis.

The model leverages data products from other DECARB tasks, primarily those estimating high resolution spatial 
and temporal demand load profiles: ResStock, ComStock, Scout, dGEN, ReEDS, and TEMPO. 
ResStock/ComStock outputs are used to estimate existing building electrical capacity and the 
load associated with the building decarbonization technologies. Adoption forecasts for the various 
technologies are sourced from the other models.

![Basic overview of BICEP model framework.](model_overview.png "BICEP Model Overview")