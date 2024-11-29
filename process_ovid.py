import pandas as pd 
import rispy
import os 
from embase_process_ris import preprocess_text, calculate_similarity
from article_retrieval import process_api_results

embase_results_path = os.path.join('title_searches/embase_2nd_titlesearch.ris')
pubmed_results_path = os.path.join('title_searches/pubmed_2nd_titlesearch.ris')

doi_pmid_results_path = 'retrieval_results/api_retrieved.xlsx'

#read in matching titles 
unsucessful_pubmed = pd.read_excel(doi_pmid_results_path, sheet_name='unsucessful_retrieve_pubmed')
unsucessful_embase = pd.read_excel(doi_pmid_results_path, sheet_name='unsucessful_retrieve_embase')
oa_results = pd.read_excel(doi_pmid_results_path, sheet_name = 'api_results_oa')
emb_results = pd.read_excel(doi_pmid_results_path, sheet_name = 'api_results_embase')
pubmed_results = pd.read_excel(doi_pmid_results_path, sheet_name = 'api_results_pubmed')
ss_results = pd.read_excel(doi_pmid_results_path, sheet_name = 'api_results_ss')

#consolidate all retrieved titles into 1 
title_template_df = oa_results.copy()
title_template_df['title'] = title_template_df['title'].fillna(emb_results['title'])
title_template_df['title'].fillna(pubmed_results['title'], inplace=True)
title_template_df['title'].fillna(ss_results['title'], inplace=True)


unsucessful_pubmed['title_matching'] = (
    unsucessful_pubmed['title']
    .fillna(unsucessful_pubmed['title_if_unavailable'])
    .fillna(title_template_df['title'])
)

unsucessful_embase['title_matching'] = (
    unsucessful_embase['title']
    .fillna(unsucessful_embase['title_if_unavailable'])
    .fillna(title_template_df['title'])
)


#read in original results for merging later 
og_embase = pd.read_excel(doi_pmid_results_path, sheet_name='api_results_embase')
og_pubmed = pd.read_excel(doi_pmid_results_path, sheet_name='api_results_pubmed')


# Load Embase results
with open(embase_results_path, 'r', encoding='utf-8') as ris_file: 
    emb_second_results = rispy.load(ris_file, skip_unknown_tags=True)
# Convert to DataFrame
emb_second_results_df = pd.DataFrame(emb_second_results)
emb_second_results_df.rename(
    columns = {
        'id':'pmid_retrieved', 
        'doi':'doi_retrieved', 
        'accession_number' : 'api_id_retrieved',
        'primary_title' : 'title_2ndsearch',
        'abstract' : 'abstract_2ndsearch'
        }, inplace=True)

# Load PubMed results
with open(pubmed_results_path, 'r', encoding='utf-8') as ris_file: 
    pubmed_second_results = rispy.load(ris_file, skip_unknown_tags=True)

# Convert to DataFrame - second round search 
pubmed_second_results_df = pd.DataFrame(pubmed_second_results)
pubmed_second_results_df.rename(
    columns = {
        'first_authors' : 'authors', 
        'id' : 'api_id_retrieved', 
        'primary_title' : 'title_2ndsearch', 
        'notes_abstract' : 'abstract_2ndsearch', 
        'place_published' : 'venue', 
    }, inplace = True
)

#drop columns in resuls that are not in original pubmed results 
col_interest = ['api_id_retrieved', 'title_2ndsearch', 'abstract_2ndsearch', 'publication_year', 'venue', 'doi', 'authors']
pubmed_second_results_df_interest = pubmed_second_results_df[col_interest]


#perform matching - title on title 
def compare_og_results(original_df, result_df): 

    def find_match(row, result_df, result_col,  og_col): 
        sim_score = result_df[result_col].apply(lambda x : calculate_similarity(row[og_col], x))
        best_match_index = sim_score.idxmax() 
        return pd.Series({
        'best_match_index': best_match_index,
        'similarity_score': sim_score.max(),
        'best_match_string': row[og_col],
        'matching_title_from_search_results': result_df.loc[best_match_index, result_col],
        'included_article_id': row['included_article_id']
    })

    #find best matching results from second round search 
    best_matches_df = original_df.apply(lambda row: find_match(row, result_df, 'title_2ndsearch', 'title_matching'), axis=1)
    best_matches_df = best_matches_df.reset_index()
    # Rename the 'index' column to 'original_index' for clarity
    best_matches_df = best_matches_df.rename(columns={'index': 'original_index'})
    best_matches_df = best_matches_df[best_matches_df['similarity_score'] >= 90]

    #best match index corresponds with the index of result_df that needs to be merged with original_df  
    result_df_best_match = result_df.loc[best_matches_df['best_match_index']]
    best_match_df_indexed = best_matches_df.set_index('best_match_index')
    result_df_best_match['matching_index'] = best_match_df_indexed['original_index']
    result_df_best_match = result_df_best_match.set_index('matching_index')


    result_df_best_match['search_round'] = 'round2'
    original_df['search_round'] = 'round1'
    #merge back with original_df 
    updated_df = original_df.combine_first(result_df_best_match)

    #drop duplicates baed on ibcluded_article_id 
    updated_df.drop_duplicates(subset = 'included_article_id', keep = 'first', inplace = True)
    updated_df_indexed = updated_df.set_index('included_article_id')
    
    return updated_df_indexed


#takes in unsucessful search results and tries to add information from second round search 
print('Processing embase second round results')
unsuccessful_embase_updated = compare_og_results(unsucessful_embase, emb_second_results_df)
og_embase_indexed = og_embase.set_index('included_article_id')
og_embase_updated = og_embase_indexed.combine_first(unsuccessful_embase_updated)
og_embase_updated.reset_index(inplace =True)

print('Processing pubmed second round results')
unsuccessful_pubmed_updated = compare_og_results(unsucessful_pubmed, pubmed_second_results_df_interest)
#merge back with og 
og_pubmed_indexed = og_pubmed.set_index('included_article_id')
og_pubmed_updated =og_pubmed_indexed.combine_first(unsuccessful_pubmed_updated)
og_pubmed_updated.reset_index(inplace =True)

file_path = 'retrieval_results/api_retrieved_pubmed_embase_2ndsearch.xlsx'
with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
    _ = process_api_results(og_pubmed_updated, api_name = 'pubmed', writer = writer)
    _ = process_api_results(og_embase_updated, api_name = 'embase', writer = writer)




