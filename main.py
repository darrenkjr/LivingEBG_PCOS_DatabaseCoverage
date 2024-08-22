from article_retrieval import *
import pandas as pd 

excel_path = '../PCOS_Guideline_Dataset.xlsm'
df = pd.read_excel(excel_path, sheet_name="included_articles")

#go through the id list 
id_col = ['included_article_doi', 'included_article_pmid']
#fill in oa id first 

semanticscholar_api_key =  os.getenv('semantic_scholar_api_key')
#initiate instances 
oa_instance = openalex_interface() 
ss_instance = semanticscholar_interface(semanticscholar_api_key)
pubmed_instance = async_metapub_wrapper()
scopus_instance = scopus_interface()

file_path = 'retrieval_results/api_retrieved.xlsx'


with pd.ExcelWriter(file_path, engine='openpyxl') as writer:

    pmid_input_df = df.copy()
    pmid_input_df['id_sent_apiretrieval'] = None
    
    # OpenAlex and Semantic Scholar retrieval
    for api, api_name in zip([oa_instance, ss_instance], ['oa', 'ss']):
        print(f'Retrieving ids from {api_name}')
        df_results, _ = retrieve_ids(df, api)
        retrieved_pmids = process_api_results(df_results, api_name, writer)
        pmid_input_df['id_sent_apiretrieval'] = pmid_input_df['id_sent_apiretrieval'].fillna(retrieved_pmids)
    
    # Scopus and PubMed retrieval
    for api, api_name in zip([scopus_instance, pubmed_instance], ['scopus', 'pubmed']):
        print(f'Retrieving from {api_name.capitalize()} API...')
        input_df = pmid_input_df.copy()
        input_df['id_sent_apiretrieval'] = input_df['id_sent_apiretrieval'].fillna(input_df['included_article_doi'])
        input_df['id_sent_apiretrieval'] = input_df['id_sent_apiretrieval'].str.lower().str.replace(" ", "")
        df_results, _ = retrieve_ids(input_df, api)
        retrieved_pmids = process_api_results(df_results, api_name, writer)