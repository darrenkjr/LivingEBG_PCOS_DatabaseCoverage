# Assessing the Coverage of PubMed, Embase, OpenAlex and Semantic Scholar for Automated Single Database Searches in Living Guideline Evidence Surveillance : A Case Study of the International PCOS Guidelines 2023

This project is designed to retrieve and process scientific article data from various APIs including OpenAlex, Semantic Scholar, and PubMed. The data and code allows replication of our automated retrieval and analyses as part of our paper: "Assessing the Coverage of PubMed, Embase, OpenAlex and Semantic Scholar for Automated Single Database Searches in Living Guideline Evidence Surveillance : A Case Study of the International PCOS Guidelines 2023, Currently under review" 


## Installation

Requirements: 
* Python 3.12.4 (Virtual Environment Preferred)
* API Keys for PubMed, OpenAlex and Semantic Scholar (if planning on rerunning the experiment)

1. Clone the repository
2. Install pixi (https://docs.pixi.dev/getting-started/installation)
3. Run `pixi install` to install the dependencies

### To Check Results and Recreate Figures 

1. Check src/retrieval_results/api_retrieved_final.xlsx for top line results. Tabs with XX_metrics provide results for each database
2. Consult src/analysis/all_unsuccessful_nodupe_rob_FIXED.csv for list of articles that were unsucessfully retrieved (with ROB scores)
3. Run src/analysis/analysis.ipynb to regenerate api_retrieved_final.xlsx and all_unsuccessful_nodupe_rob_FIXED.csv and recreate figures (overlap between databasesa and ROB sensitivity analysis) 



### To Rerun the Experiment


1. Set up your API keys in a .env file (see .env.example for required keys)
2. Run src/main.py 
3. Run src/analysis/analysis.ipynb
