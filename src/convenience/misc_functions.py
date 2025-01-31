import pandas as pd
import unicodedata
from api_interfaces.embase_search_generator import generate_title_search_files
import re
from fuzzywuzzy import fuzz


def generate_second_round_title_searches():
    folder = 'title_searches/'
    embase = pd.read_csv(folder + 'title_search_embase_fixed.csv')
    pubmed = pd.read_csv(folder + 'title_search_pubmed_fixed.csv')

    embase['title'] = embase['title'].apply(lambda x : preprocess_text(x))
    pubmed['title'] = pubmed['title'].apply(lambda x : preprocess_text(x))

    generate_title_search_files(embase, folder, 'embase')
    generate_title_search_files(pubmed, folder, 'pubmed')

def fill_titles():
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



#reads in results from embase and also the ids sent to embase, and them merges them 
def clean_doi(doi):
    if pd.isna(doi) or doi == '':
        return ''
    doi = str(doi).lower().strip()
    return re.sub(r'^https?://(?:dx\.)?doi\.org/', '', doi)



# Step 1: Create a function to calculate similarity
def calculate_similarity(str1, str2):
    # Preprocess both strings
    str1_processed = preprocess_text(str1)
    str2_processed = preprocess_text(str2)
    # Calculate similarity using fuzz.ratio
    return fuzz.ratio(str1_processed, str2_processed)
# Step 2: Prepare the data


# Step 3 & 4: Calculate similarities and find the best match
def find_best_match(row, result_df, col):
    sim_score = result_df['match_string'].apply(lambda x: calculate_similarity(row[col], x))
    best_match_index = sim_score.idxmax()
    return pd.Series({
        'best_match_index': best_match_index,
        'similarity_score': sim_score.max(),
        'best_match_string': row['included_reference'],
        'matching_title_from_search_results': result_df.loc[best_match_index, 'primary_title'],
        'included_article_id': row['included_article_id']
    })

def combine_columns(row):
    try:
        # Start with primary_title
        combined = str(row['primary_title']) if pd.notna(row['primary_title']) else ''
        
        # Add alternate_title3 if it exists
        if pd.notna(row['alternate_title3']):
            combined += ' ' + str(row['alternate_title3'])
        
        # Add publication_year if it exists
        if pd.notna(row['publication_year']):
            year = str(row['publication_year']).replace('//', '')
            combined += ' (' + year + ')'

        # Add first_authors if it exists
        if isinstance(row['first_authors'], list) and len(row['first_authors']) > 0:
            combined += ' ' + ' '.join(row['first_authors'])
        elif pd.notna(row['first_authors']):
            combined += ' ' + str(row['first_authors'])
        
        return combined.strip()
    
    except Exception as e:
        print(f"Error processing row: {row}")
        print(f"Error message: {str(e)}")
        return ''
    


def preprocess_text(text):
    if pd.isna(text):
        return ''
    # Convert to string if not already
    text = str(text)
    # Normalize Unicode characters
    text = unicodedata.normalize('NFKD', text)
    # Convert to ASCII, ignoring non-ASCII characters
    text = text.encode('ascii', 'ignore').decode('ascii')
    # Convert to lowercase
    text = text.lower()
    # Remove non-alphanumeric characters (except spaces)
    text = re.sub(r'[^a-z0-9\s]', ' ', text)
    # Remove extra whitespace
    text = ' '.join(text.split())
    return text


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
    best_matches_df = best_matches_df[best_matches_df['similarity_score'] >= 80]
    
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
    