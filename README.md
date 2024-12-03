# Assessing the Coverage of PubMed, Embase, OpenAlex and Semantic Scholar for Automated Single Database Searches in Living Guideline Evidence Surveillance : A Case Study of the International PCOS Guidelines 2023

This project is designed to retrieve and process scientific article data from various APIs including OpenAlex, Semantic Scholar, and PubMed. The data and code allows replication of our automated retrieval and analyses as part of our paper: "Assessing the Coverage of PubMed, Embase, OpenAlex and Semantic Scholar for Automated Single Database Searches in Living Guideline Evidence Surveillance : A Case Study of the International PCOS Guidelines 2023"


## Installation

Requirements: 
* Python 3.12.4 (Virtual Environment Preferred)
* API Keys for PubMed, OpenAlex and Semantic Scholar (if planning on rerunning the experiment)

1. Clone the repository
2. Install the required packages:
   ```
   pip install -r requirements.txt
   ```

### To Check Results and Recreate Figures 

1. Check retrieval_results/api_retrieved_final.xlsx for top line results. Tabs with XX_metrics provide results for each database
2. Run analysis/analysis.ipynb to check further analyses and recreate figures (overlap between databasesa and ROB sensitivity analysis) 



### To Rerun the Experiment (Will Overwrite Current Results and requires manual intervention at points)


1. Set up your API keys in a .env file (see .env.example for required keys)
2. Run pmid_doi_search.py to retrieve articles from the APIs (PubMed, Semantic Scholar and OpenAlex)
3. Embase search results are available in gdg_embase_searches, run embase_process_ris.py to parse results (deposited in retrieval_results/api_retrieved_embase.xlsx)
4. Run fill_titles.py and then generate_titlesearch.py to generate second round title only search to be run via Medline (results in title_searches/) 
5. Run process_ovid.py to parse results of second round title only search (deposited in retrieval_results/api_retrieved_pubmed_embase_2ndsearch.xlsx)
6. Combine updated tabs from api_retrieved_pubmed_embase_2ndsearch.xslsx with api_retrieved.xlsx to regenerate api_retrieved_final.xlsx (tabs with XX_metrics)
