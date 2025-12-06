This file provides Claude Code guidance when working with code in this repo.

## Project Overview

FDIC Data Pipeline -- automates downloading, processing and summarizing of data made available via the FDIC api (https://api.fdic.gov/banks/docs/#/). 

Specifically, the structure data located at the /banks/institutions and the failure data located at /banks/failures. 

Each of these has an associated .yaml file with variable information. Ideally we would incorporate these variable descriptions into our parsed data much like STATA datasets will have a description associated with each variable.

Reference another repo data_sod that used the same api to organize this project with similar steps and files. Note, that did not incorporate the .yaml file (I am working on that in paralell.) 



