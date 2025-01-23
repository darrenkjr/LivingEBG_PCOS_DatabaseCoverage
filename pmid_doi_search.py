from article_retrieval import *
import pandas as pd 

excel_path = 'PCOS_Guideline_Dataset.xlsm'
df = pd.read_excel(excel_path, sheet_name="included_articles", dtype={'included_article_pmid': str})
#assume that same_study_diff_articles (none = primary_citations)
df['same_study_diff_article'] = df['same_study_diff_article'].fillna('primary_citation')
df = df[df['included_postfulltext'] != 0]
#go through the id list 
id_col = ['included_article_doi', 'included_article_pmid']
#fill in oa id first 

semanticscholar_api_key =  os.getenv('semantic_scholar_api_key')
#initiate instances 
oa_instance = openalex_interface() 
ss_instance = semanticscholar_interface(semanticscholar_api_key)
pubmed_instance = async_metapub_wrapper()

file_path = 'retrieval_results/api_retrieved.xlsx'


with pd.ExcelWriter(file_path, engine='openpyxl') as writer:

    
    scopus_pubmed_input_df = df.copy()
    #add pmid: to non empty pmid 
    #set up id_sent_apiretrieval 
    scopus_pubmed_input_df['id_sent_apiretrieval'] = scopus_pubmed_input_df['included_article_pmid'].apply(lambda x: f'pmid:{x}' if not pd.isna(x) else x)
    # OpenAlex and Semantic Scholar retrieval
    for api, api_name in zip([oa_instance, ss_instance], ['oa', 'ss']):
        print(f'Retrieving ids from {api_name}')
        df_results, _ = retrieve_ids(df, api)
        retrieved_ids = process_api_results(df_results, api_name, writer)
    
        if 'included_article_id' in retrieved_ids.columns and 'included_article_id' in scopus_pubmed_input_df.columns:
            # Merge based on the common identifier
            merged_df = scopus_pubmed_input_df.merge(
                retrieved_ids[['included_article_id', 'pmid', 'doi']], 
                on='included_article_id', 
                how='left',
            )
            
            #complete id_sent_apiretrieval - with pmid, then doi 
            merged_df['id_sent_apiretrieval'] = merged_df['id_sent_apiretrieval'].fillna(merged_df['pmid'])
            merged_df['id_sent_apiretrieval'] = merged_df['id_sent_apiretrieval'].fillna(merged_df['doi'])
    
    # Scopus and PubMed retrieval
    for api, api_name in zip([pubmed_instance], ['pubmed']):
        print(f'Retrieving from {api_name.capitalize()} API...')
        merged_df['id_sent_apiretrieval'] = merged_df['id_sent_apiretrieval'].str.lower().str.replace(" ", "")
        df_results, _ = retrieve_ids(merged_df, api)

        #placecholder varialbe as we are not using this output 
        _ = process_api_results(df_results, api_name, writer)