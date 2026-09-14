We need to define the user experience very clearly - because params need to be passed.

We need to strongly consider the user experience here, how will customers answer those
business questions? 

We maybe need to figure out some usage patterns for the data registry before defining 
how event emission will work

Marquez is not customer friendly so not sure how Feast will do it


So the accurate conclusion is:
Marquez contains the lineage evidence needed to build the end-to-end linked graph, but a polished registry-aware customer experience would need to be derived from Marquez and Registry API data and presented in a custom RHOAI UI.

What is not currently present is immutable source evidence or registry-to-column-level lineage. The current data supports the assurance level Linked, not Observed or Reproducible.


This is an important architectural distinction: “Feast can consume OpenLineage”
is a backend capability; “a user can understand the history and impact of a Data
Registry asset” is the product outcome still needing a contract.

Is there a decision made on Marquez versus Feast


We might need to consider a service that pulls together all APIs to answer the business level questions.

Which components do we need to support initially? 
How will this work? Do we expect users to manually pass params, such as parentRunId to child components. 
If not we may need to adjust all components 



I want to create a summary document that I can provide to the author of the ADR and the questions and answers basically Anna and Nikhil. I basically want to summarise my findings succinctly focusing on maybe a few of the key points. The first point I think is worth making is around defining a much clearer use cas user journeys and DN state business use cases such as the questions we defined as something that needs to be done. The next point I'd like to make is around the clear distinction between data and age and operational lineage. Regarding user journeys I think this will help in defining which components we need to consider for open lineage.
correlations !! 
The need for policies 


Feast can replace Marquez as the generic operational-lineage store only if RHOAI adds and verifies the missing identity, authorization, retention, evidence, and asset-centric API contracts around it.