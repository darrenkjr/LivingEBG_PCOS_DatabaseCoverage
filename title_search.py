import pandas as pd 
import os 
from api_interfaces.async_metapub_wrapper import async_metapub_wrapper
import asyncio 


doi_pmid_results_path = 'retrieval_results/api_retrieved_1.xlsx'
 
#unsucessful scopus 
unsucessful_scopus = pd.read_excel(doi_pmid_results_path, sheet_name='unsucessful_retrieve_scopus')
unsucessful_pubmed = pd.read_excel(doi_pmid_results_path, sheet_name='unsucessful_retrieve_pubmed')

compiled_results_oa = pd.read_excel(doi_pmid_results_path, sheet_name='api_results_oa')
results_ss = pd.read_excel(doi_pmid_results_path, sheet_name='api_results_ss')

results_ss[['biblio_journal', 'biblio_volume', 'biblio_pages']] = results_ss['journal'].apply(lambda x: pd.Series({
    'biblio_journal': x.get('name', '').strip() if x.get('name') else None,
    'biblio_volume': x.get('volume', '').strip() if x.get('volume') else None,
    'biblio_pages': x.get('pages', '').strip() if x.get('pages') else None
}))


compiled_results_oa['title'] = compiled_results_oa['title'].fillna(compiled_results_oa['title_if_unavailable']).fillna(results_ss['title'])


#print included_reference where title is empty 
ss_result_set = set(results_ss['included_article_id'])
oa_result_set = set(compiled_results_oa['included_article_id'])

#find the difference between the two sets 
difference = oa_result_set - ss_result_set

#print the difference
print(difference)

unsucessful_pubmed['title_sent'] = unsucessful_pubmed['title'].fillna(compiled_results_oa['title'])
#retrieve pubmed articles based on title sent 
async_metapub_instance = async_metapub_wrapper()
pubmed_title_search_results = asyncio.run(async_metapub_instance.async_fetch_pubmed_articles(unsucessful_pubmed, title_search_flag=True))
print('Title seach results done, saving as csv')
pubmed_title_search_results.to_csv('retrieval_results/pubmed_title_search_results.csv', index=False)






