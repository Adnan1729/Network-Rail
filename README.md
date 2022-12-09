# Network-Rail
Summer Internship 2023

**1	II R&D background and document summary**

The Research & Development (R&D) Innovation stream is a key component of the NR Intelligent Infrastructure (II) programme. The R&D programme is focused on using Advanced Analytics and Machine Learning (ML) POC's to improve the prediction and prevention of faults. The overall approach is Use Case based with initially a maximum of 6 Use Cases in scope, now extended to 8.   Several of the Use Cases have a number of Phases. This document covers Phase 1, Phase 2 and Phase 3 changes (HW series changes, model performance experiment) of the Point Operating Equipment use case. Those business users familiar with POE Phase 1 solution document may wish to jump to section 1.9 Modelling Results. Colleagues who are engaged on converting model output to Prototype and Production may wish to go to section 4, Data Requirements and Data Ingestion.

**1.1	Innovation stream aim & objectives**

**Aim** <br />

To refine and build fault prediction analytics products for Track & Signalling workstreams.

**Objectives:**
•	Deliver analytics solutions for the Track and Signalling workstreams to enable maintenance managers to determine an intervention based on a prediction within the Decision Support Tool (DST).
•	To predict key faults that will support making the correct intervention to reduce delays, while ensuring that the safety standards are met.
•	Deliver innovative R&D to both Track and Signalling business workstreams.
•	Deliver proof of concept (POC) outcomes to inform Track and Signalling workstreams with the ‘Art of the Possible’.

**1.2	Point operating equipment problem statement**

To predict POE failures so that proactive intervention can be planned before potential disruption in service occurs. Prediction on failure modes for POE ideally needs to be made at least 2 days ahead of its failure to ensure that the intervention can be planned safely.  POE assets for which Wonderware data is available were considered for this exercise.


**1.3	Success criteria**

The initial success criterion of the Use Case was that POE failures should be predicted ideally at least 2 days ahead of its failure.  This was modified following initial exploratory analysis to the following: •	Predictions, particularly those 2 weeks or more in advance,   should be better than the existing alerts and alarms that are configured using Wonderware data. 

**1.4	Document scope**

The scope of this document is to present the methodology and results from POE Phase 1 (to 28th September 2020), POE Phase 2 (28th September to 13th November) and Phase 3 (ongoing)
 
**1.5	Approach for point operating equipment use case**

As soon as the Innovation workstream began, Programme Management Team organised a series of workshops for key Subject Matter Experts (SMEs) and Business Users. The Business / Data Analysis (BA/DA) team took the output, defined the scope, refined the problem statement and identified all the relevant data sources. Data Engineering (DE) Team constructed a pipeline to ingest the data and Data Scientists (DS) performed Exploratory Data Analysis (EDA), worked with DE team on feature engineering and experimented with a number of different models. 

A full description of the BA/DA, DE and DS processes are provided in subsequent sections of this document. Phase 1 considered a well-defined set of assets, faults and ELRs. 

Phase 2 of POE (this phase) concentrated on considering additional factors that may affect the health of a POE asset, and on refining the models to reduce both false positive and false negative rates while maintaining precision and accuracy and extending the range of assets included. 

During Phase 3, the team focussed on finalising the feature set and building supervised model.

**1.6	Underlying assumptions**

In the absence of a predefined label of the degradation state of an asset, the pattern of the trace of the electrical current through an asset (both Track Circuit and Point Operating Equipment (POE)) may be analysed using advanced analytical methods and the differences in the patterns used to identify faults from non-faults.
All the mentioned Data science solutions from the R&D team should only be treated as a POC (Proof of concept). The workstream team should do applicable analysis and changes to these solutions for making it production ready.
