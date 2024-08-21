from api_interfaces.openalex import openalex_interface
from api_interfaces.semanticscholar import semanticscholar_interface
from api_interfaces.async_metapub_wrapper import async_metapub_wrapper
from dotenv import load_dotenv
load_dotenv() 
import pandas as pd 
import numpy as np 
import asyncio 
import os 



def percentageretrieved_calc(df): 
    # Determine retrieval failures
    
    df['api_retrieval_success'] = np.where(
        (df['included_postfulltext'] != 0) & (~pd.isna(df['api_id_retrieved'])), 1, 0
    )

    # Filter the dataframe to exclude zero `included_postfulltext`
    df_nonzero_review = df[df['included_postfulltext'] != 0]
    num_included_articles = df_nonzero_review.shape[0]

    # Count number of input IDs sent for retrieval
    num_input_ids_retrievalsent = df_nonzero_review[
        (df_nonzero_review['included_postfulltext'] != 0) &
        (df_nonzero_review['id_sent_apiretrieval'] != 'no_id_provided')
    ].shape[0]

    # Count the number of included articles
    
    
    # Calculate percentages
    if num_input_ids_retrievalsent > 0:
        percentage_retrieved = ((df['api_retrieval_success'].sum()) / num_input_ids_retrievalsent) * 100
    else:
        percentage_retrieved = 0

    if num_included_articles > 0:
        percentage_retrieved_overall = ((df['api_retrieval_success'].sum()) / num_included_articles) * 100
    else:
        percentage_retrieved_overall = 0

    return {
        'num_input_ids_retrievalsent': num_input_ids_retrievalsent,
        'num_included_articles': num_included_articles,
        'percentage_retrieved': percentage_retrieved,
        'percentage_retrieved_overall': percentage_retrieved_overall
    }


def retrieve_ids(df, api_instance): 
    df_copy = df.copy() 
    if isinstance(api_instance, async_metapub_wrapper): 
        retrieval_results = asyncio.run(api_instance.async_fetch_pubmed_articles(df_copy))


    # if not pubmed -> use custom openalex or semantic scholar interfaces 
    else: 
        retrieval_results = asyncio.run(api_instance.retrieve_generic_paper_details(df_copy))

    api_fail_df = retrieval_results[pd.isna(retrieval_results['api_id_retrieved'])]
    #handle instances where api retrived 2 results for 1 API ID - first compare length of df_copy with df
    if df_copy.shape[0] != retrieval_results.shape[0]:

        print('Potential duplicate entity found, 1 result retrieved for 1 input id')
        #find duplicate based on original input rows 
        dupe_check_col = retrieval_results.columns[:8]
        duplicate_rows = retrieval_results[retrieval_results.duplicated(subset=dupe_check_col, keep=False)]
        retrieval_results = retrieval_results.drop(duplicate_rows.index)
        try:
            rows_to_keep = duplicate_rows.loc[
                duplicate_rows.groupby(list(dupe_check_col))['citation_network_size'].idxmax()
        ]
        except KeyError: 
            #if doing by citation network size is not possible - just get the first row 
            rows_to_keep = duplicate_rows.loc[
                duplicate_rows.groupby(list(dupe_check_col)).head(1).index
        ]
        # Add back the rows with the highest citation_network_size
        retrieval_results = pd.concat([retrieval_results, rows_to_keep]).sort_index()
        #check shape again 
        print(f"Original input df length: {df_copy.shape[0]}, Length after deduplication: {retrieval_results.shape[0]}")

        # Reindex to preserve the original order
        retrieval_results = retrieval_results.sort_index() 
    return retrieval_results, api_fail_df


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

file_path = 'api_retrieved.xlsx'


with pd.ExcelWriter(file_path, engine='openpyxl') as writer:

    pmid_input_df = df.copy()
    pmid_input_df['id_sent_apiretrieval'] = None
    for api, api_name in zip([oa_instance,ss_instance], ['oa','ss']):
        print(f'Retrieving ids from {api_name}')
        df_results, api_fail_df = retrieve_ids(df, api)
            # Calculate metrics for the entire DataFrame
        overall_metrics = percentageretrieved_calc(df_results)
        overall_metrics_df = pd.DataFrame([overall_metrics], index=['Overall'])

        # # Calculate metrics for each group
        grouped_metrics = df_results.groupby('question_id').apply(percentageretrieved_calc).apply(pd.Series)
        grouped_metrics_gdg = df_results.groupby('GDG').apply(percentageretrieved_calc).apply(pd.Series)

        # # Combine overall and grouped metrics
        metrics_df = pd.concat([overall_metrics_df, grouped_metrics_gdg, grouped_metrics])
        #write to excel worksheet 
        df_results.to_excel(writer, sheet_name=f"api_results_{api_name}", index=False)
        metrics_df.to_excel(writer, sheet_name=f"metrics_{api_name}", index=False)
        api_fail_df.to_excel(writer, sheet_name = f"unsucessful_retrieve_{api_name}", index = False)

        #extract pmid that were retrieved from openalex and semantic scholar - add to df to be verified later
        pmid_input_df['id_sent_apiretrieval'] = pmid_input_df['id_sent_apiretrieval'].fillna(df_results['pmid'])
    
    #now, do pubmid 
    print('Retrieving from PubMed API..')
    api_name = 'pubmed'
    pmid_input_df['id_sent_apiretrieval'] = pmid_input_df['id_sent_apiretrieval'].fillna(pmid_input_df['included_article_doi'])
    pmid_input_df['id_sent_apiretrieval'] = pmid_input_df['id_sent_apiretrieval'].str.lower().str.replace(" ", "")
    df_results, api_fail_df = retrieve_ids(pmid_input_df, pubmed_instance)
    overall_metrics = percentageretrieved_calc(df_results)
    overall_metrics_df = pd.DataFrame([overall_metrics], index=['Overall'])

    # # Calculate metrics for each group
    grouped_metrics = df_results.groupby('question_id').apply(percentageretrieved_calc).apply(pd.Series)
    grouped_metrics_gdg = df_results.groupby('GDG').apply(percentageretrieved_calc).apply(pd.Series)

    # # Combine overall and grouped metrics
    metrics_df = pd.concat([overall_metrics_df, grouped_metrics_gdg, grouped_metrics])

    df_results.to_excel(writer, sheet_name=f"api_results_{api_name}", index=False)
    metrics_df.to_excel(writer, sheet_name=f"metrics_{api_name}", index=False)
    api_fail_df.to_excel(writer, sheet_name = f"unsucessful_retrieve_{api_name}", index = False)

    


