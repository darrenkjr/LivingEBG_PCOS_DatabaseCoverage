import pandas as pd 
from api_interfaces.embase_search_generator import generate_title_search_files


doi_pmid_results_path = 'retrieval_results/api_retrieved.xlsx'
 
unsucessful_pubmed = pd.read_excel(doi_pmid_results_path, sheet_name='unsucessful_retrieve_pubmed').set_index('included_article_id')
unsucessful_embase = pd.read_excel(doi_pmid_results_path, sheet_name='unsucessful_retrieve_embase').set_index('included_article_id')

oa_results = pd.read_excel(doi_pmid_results_path, sheet_name = 'api_results_oa').set_index('included_article_id')
ss_results = pd.read_excel(doi_pmid_results_path, sheet_name = 'api_results_ss').set_index('included_article_id')

oa_results['title'].fillna(ss_results['title'], inplace=True)


unsucessful_pubmed['title'] = unsucessful_pubmed['title'].fillna(unsucessful_pubmed['title_if_unavailable'])
unsucessful_pubmed['title'].fillna(oa_results['title'], inplace=True)

unsucessful_embase['title'] = unsucessful_embase['title'].fillna(unsucessful_embase['title_if_unavailable'])
unsucessful_embase['title'].fillna(oa_results['title'], inplace=True)

base_output_dir = 'title_searches/'

unsucessful_pubmed.to_csv(base_output_dir + 'title_search_pubmed.csv')
unsucessful_embase.to_csv(base_output_dir + 'title_search_embase.csv')












