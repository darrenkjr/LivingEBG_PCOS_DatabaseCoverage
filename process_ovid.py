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
#set index of title_template_df 
title_template_df.set_index('included_article_id', inplace = True)
emb_results.set_index('included_article_id', inplace = True)
pubmed_results.set_index('included_article_id', inplace = True)
ss_results.set_index('included_article_id', inplace = True)

title_template_df['title'] = title_template_df['title'].fillna(emb_results['title'])
title_template_df['title'].fillna(pubmed_results['title'], inplace=True)
title_template_df['title'].fillna(ss_results['title'], inplace=True)

#set index of title_template_df 
unsucessful_pubmed.set_index('included_article_id', inplace = True)
unsucessful_embase.set_index('included_article_id', inplace = True)

#empty title_matching column 
unsucessful_pubmed['title_matching'] = unsucessful_pubmed['title_if_unavailable']
unsucessful_pubmed['title_matching'].fillna(title_template_df['title'], inplace = True)

#set index 
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
        'notes_abstract' : 'abstract_2ndsearch'
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

def find_match(row, result_df, result_col,  og_col): 
    #reset_index result_df 
    
    
    sim_score = result_df[result_col].apply(lambda x : calculate_similarity(row[og_col], x))
    best_match_index = sim_score.idxmax() 
    return pd.Series({
    'best_match_index': best_match_index,
    'similarity_score': sim_score.max(),
    'best_match_string': row[og_col],
    'matching_title_from_search_results': result_df.loc[best_match_index, result_col],
    # 'included_article_id': row['included_article_id'],
    'included_reference': row['included_reference']
})    


#perform matching - title on title 
def compare_og_results(original_df, result_df): 

    original_df = original_df.reset_index()
    included_article_ids = original_df['included_article_id'].copy()
    
    # Find matches using title similarity
    best_matches_df = original_df.apply(
        lambda row: find_match(row, result_df, 'title_2ndsearch', 'title_matching'), 
        axis=1
    )
    
    # Filter for good matches (similarity >= 90)
    best_matches_df = best_matches_df[best_matches_df['similarity_score'] >= 90]
    
    # Get matching results from second round search based on the index at which the best match is found 
    result_df_matched = result_df.loc[best_matches_df['best_match_index']].copy()
    
    # Set the correct index for merging
    result_df_matched.index = best_matches_df.index
    
    # Add round indicators
    result_df_matched['search_round'] = 'round2'
    original_df['search_round'] = 'round1'
    
    # Set index back to included_article_id for final merge
    original_df.set_index('included_article_id', inplace=True)
    result_df_matched.index = included_article_ids[result_df_matched.index]
    
    # Combine and return
    return original_df.combine_first(result_df_matched)
    

#takes in unsucessful search results and tries to add information from second round search 
# Process embase results
print('processing embase results')
unsuccessful_embase_updated = compare_og_results(unsucessful_embase, emb_second_results_df)
og_embase_updated = og_embase.set_index('included_article_id').combine_first(unsuccessful_embase_updated)
og_embase_updated.reset_index(inplace=True)

# Process pubmed results  
print('processing pubmed results')
unsuccessful_pubmed_updated = compare_og_results(unsucessful_pubmed, pubmed_second_results_df_interest)
og_pubmed_updated = og_pubmed.set_index('included_article_id').combine_first(unsuccessful_pubmed_updated)
og_pubmed_updated.reset_index(inplace=True)

file_path = 'retrieval_results/api_retrieved_pubmed_embase_2ndsearch.xlsx'
with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
    _ = process_api_results(og_pubmed_updated, api_name = 'pubmed', writer = writer)
    _ = process_api_results(og_embase_updated, api_name = 'embase', writer = writer)




