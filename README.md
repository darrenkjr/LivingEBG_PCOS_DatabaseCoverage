# Assessing the Coverage of PubMed, Embase, OpenAlex and Semantic Scholar for Automated Single Database Searches in Living Guideline Evidence Surveillance : A Case Study of the International PCOS Guidelines 2023

This project is designed to retrieve and process scientific article data from various APIs including OpenAlex, Semantic Scholar, and PubMed. The data and code allows replication of our automated retrieval and analyses as part of our paper: "Assessing the Coverage of PubMed, Embase, OpenAlex and Semantic Scholar for Automated Single Database Searches in Living Guideline Evidence Surveillance : A Case Study of the International PCOS Guidelines 2023"

## Installation

1. Clone the repository
2. Install the required packages:
   ```
   pip install -r requirements.txt
   ```
3. Set up your API keys in a .env file (see .env.example for required keys)
4. Run retrieve_articles.py to retrieve articles from the APIs (PubMed, Semantic Scholar and OpenAlex)
6. Embase search results are available in gdg_embase_searches, run embase_process_ris.py to parse results
7. Run fill_titles.py to generate second round title only search to be run via Medline (results in title_searches/) 
8. Run process_ovid.py to parse results of second round title only search 
9. Check api_retrieved_final.xlsx (tabs with XX_metrics) for top line results 
10. Run analysis/analysis.ipynb to check further analyses (overlap between databasesa and ROB sensitivity analysis) 
