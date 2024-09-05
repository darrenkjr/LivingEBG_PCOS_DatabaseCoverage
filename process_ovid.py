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
ss_results = pd.read_excel(doi_pmid_results_path, sheet_name = 'api_results_ss')
oa_results['title'].fillna(ss_results['title'], inplace=True)
unsucessful_pubmed['title'] = unsucessful_pubmed['title'].fillna(unsucessful_pubmed['title_if_unavailable'])
unsucessful_pubmed['title'].fillna(oa_results['title'], inplace=True)
unsucessful_embase['title'] = unsucessful_embase['title'].fillna(unsucessful_embase['title_if_unavailable'])
unsucessful_embase['title'].fillna(oa_results['title'], inplace=True)

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
        'accession_number' : 'api_id',
        'primary_title' : 'title'
        }, inplace=True)

# Load PubMed results
with open(pubmed_results_path, 'r', encoding='utf-8') as ris_file: 
    pubmed_second_results = rispy.load(ris_file, skip_unknown_tags=True)

# Convert to DataFrame
pubmed_second_results_df = pd.DataFrame(pubmed_second_results)
pubmed_second_results_df.rename(
    columns = {
        'first_authors' : 'authors', 
        'id' : 'api_id_retrieved', 
        'primary_title' : 'title', 
        'notes_abstract' : 'abstract', 
        'place_published' : 'venue', 
    }, inplace = True
)

#drop columns in resuls that are not in original pubmed results 
col_interest = ['api_id_retrieved', 'title', 'abstract', 'publication_year', 'venue', 'doi', 'authors']
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

    best_matches_df = original_df.apply(lambda row: find_match(row, result_df, 'title', 'title'), axis=1)
    best_matches_df = best_matches_df.reset_index()
    # Rename the 'index' column to 'original_index' for clarity
    best_matches_df = best_matches_df.rename(columns={'index': 'original_index'})
    best_matches_df = best_matches_df[best_matches_df['similarity_score'] >= 90]

    #merge back with original_df 
    merged_df = best_matches_df.merge(
        original_df,
        left_on = 'best_match_index', 
        right_index = True, 
        how = 'left', 
        suffixes = ('', '_secondroundsearch'))
    
    merged_df = merged_df.set_index('included_article_id')
    original_df_indexed = original_df.set_index('included_article_id')
    original_df_indexed.update(merged_df)
    
    return original_df_updated

unsuccessful_pubmed_updated = compare_og_results(unsucessful_pubmed, pubmed_second_results_df_interest)
#merge back with og 
og_pubmed_indexed = og_pubmed.set_index('included_article_id')
og_pubmed_updated = og_pubmed_indexed.fillna(unsuccessful_pubmed_updated)
og_pubmed_updated.reset_index() 
og_pubmed_updated.rename(columns = {
    'api_id' : 'api_id_retrieved',
}, inplace = True)

unsuccessful_embase_updated = compare_og_results(unsucessful_embase, emb_second_results_df)
og_embase_indexed = og_embase.set_index('included_article_id')
og_embase_updated = og_embase_indexed.combine_first(unsuccessful_embase_updated)
og_embase_updated.reset_index() 
og_embase_updated.rename(columns = {
    'api_id' : 'api_id_retrieved'
})

file_path = 'retrieval_results/api_retrieved_pubmed_embase.xlsx'
with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
    _ = process_api_results(og_pubmed_updated, api_name = 'pubmed', writer = writer)
    _ = process_api_results(og_embase_updated, api_name = 'embase', writer = writer)




