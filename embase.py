import pandas as pd 
import rispy
import os 
from fuzzywuzzy import process, fuzz
from fuzzywuzzy.utils import full_process
import numpy as np 
import re
import unicodedata

#reads in results from embase and also the ids sent to embase, and them merges them 
def clean_doi(doi):
    if pd.isna(doi) or doi == '':
        return ''
    doi = str(doi).lower().strip()
    return re.sub(r'^https?://(?:dx\.)?doi\.org/', '', doi)

embase_search_dir = 'gdg_embase_searches/search_results/'
embase_sent_df = pd.read_csv('gdg_embase_searches/embase_ids_sent.csv')
#create a unique id column for each row 
embase_sent_df['uid']=embase_sent_df['GDG'].astype(str) + embase_sent_df['question_id'].astype(str) + embase_sent_df['included_reference'].astype(str).str[:-5]

embase_results = pd.DataFrame()
for file in os.listdir(embase_search_dir):
    if file.endswith('.ris'):
        print(f'reading file {file}')

        file_path = os.path.join(embase_search_dir, file)
        try:
            with open(file_path, 'r', encoding='utf-8') as ris_file:
                result = rispy.load(ris_file, skip_unknown_tags=True)
        except ValueError as e:
            print(f"Error reading {file}: {str(e)}")
            continue
    
        #create df from result 
        df = pd.DataFrame(result)
        #assign gdg id to each row, but just take the number 
        embase_results = pd.concat([embase_results, df], ignore_index=True)

# Reset the index if needed
#filter out rows where there were no inculded articles to begin with 
matching_df = embase_sent_df.copy()
matching_df['pmid_sent'] = matching_df['pmid_sent'].str.replace("pmid:", "").str.strip()
#process embase results 


# Clean DOI by removing both http and https prefixes, lowercasing, and stripping whitespace
embase_results['doi'] = embase_results['doi'].apply(clean_doi)
embase_results.rename(columns = {
    'id':'pmid_retrieved', 
    'doi':'doi_retrieved', 
    'accession_number' : 'api_id'
    }, inplace=True)

embase_results['pmid_retrieved'] = embase_results['pmid_retrieved'].str.replace(r'\[.*?\]', '', regex=True).str.strip()

embase_results_dedupe = embase_results.copy()
embase_results_dedupe['non_null_count'] = embase_results_dedupe.notna().sum(axis=1)
embase_results_dedupe = embase_results_dedupe.sort_values('non_null_count', ascending=False)

# Step 2: Deduplicate based on DOI and merge
embase_results_dedupe_doi = embase_results_dedupe.drop_duplicates(subset=['doi_retrieved'], keep='first')
merged_df_doi = pd.merge(
    matching_df[matching_df['doi_sent'].notna() & (matching_df['doi_sent'] != '')],
    embase_results_dedupe_doi[embase_results_dedupe_doi['doi_retrieved'].notna() & (embase_results_dedupe_doi['doi_retrieved'] != '')],
    left_on='doi_sent',
    right_on='doi_retrieved',
    how='left',
    indicator='_merge_doi',
    suffixes=('', '_embase')
)

# Step 3: Deduplicate based on PMID and merge
embase_results_dedupe_pmid = embase_results_dedupe.drop_duplicates(subset=['pmid_retrieved'], keep='first')
merged_df_pmid = pd.merge(
    matching_df[matching_df['pmid_sent'].notna() & (matching_df['pmid_sent'] != '')],
    embase_results_dedupe_pmid[embase_results_dedupe_pmid['pmid_retrieved'].notna() & (embase_results_dedupe_pmid['pmid_retrieved'] != '')],
    left_on='pmid_sent',
    right_on='pmid_retrieved',
    how='left',
    indicator='_merge_pmid',
    suffixes=('', '_embase')
)


# Step 1: Combine results from both merges
combined_df = pd.concat([merged_df_doi, merged_df_pmid])

# Step 2: Handle overlapping data
combined_df = combined_df.sort_values('non_null_count', ascending=False)
combined_df = combined_df.drop_duplicates(subset=['uid'], keep='first')
combined_df.sort_index(inplace=True)

# Step 3: Merge the combined results back to the original matching_df
# Step 3: Update matching_df with results from combined_df
print("Duplicate UIDs in matching_df:", matching_df['uid'].duplicated().sum())
print("Duplicate UIDs in combined_df:", combined_df['uid'].duplicated().sum())
# First, set 'uid' as the index for both DataFrames
matching_df_indexed = matching_df.set_index('uid')
combined_df_indexed = combined_df.set_index('uid')

# Update matching_df with values from combined_df
matching_df_indexed.update(combined_df_indexed)

# Add new columns from combined_df that don't exist in matching_df
new_columns = combined_df_indexed.columns.difference(matching_df_indexed.columns)
final_df = matching_df_indexed.join(combined_df_indexed[new_columns])

# Reset the index to make 'uid' a column again
final_df = final_df.reset_index()

# Optionally, reorder columns to ensure matching_df columns come first
matching_df_columns = matching_df.columns.tolist()
other_columns = [col for col in final_df.columns if col not in matching_df_columns]
final_df = final_df[matching_df_columns + other_columns]




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
    text = re.sub(r'[^a-z0-9\s]', '', text)
    # Remove extra whitespace
    text = ' '.join(text.split())
    return text

# Step 1: Create a function to calculate similarity
def calculate_similarity(str1, str2):
    # Preprocess both strings
    str1_processed = preprocess_text(str1)
    str2_processed = preprocess_text(str2)
    # Calculate similarity using fuzz.ratio
    return fuzz.ratio(str1_processed, str2_processed)
# Step 2: Prepare the data


# Step 3 & 4: Calculate similarities and find the best match
def find_best_match(row, result_df):
    sim_score = result_df['match_string'].apply(lambda x: calculate_similarity(row['included_reference'], x))
    best_match_index = sim_score.idxmax()
    return pd.Series({
        'best_match_index': best_match_index,
        'similarity_score': sim_score.max(),
        'best_match_string': row['included_reference'],
        'matching_title_from_embase_results': result_df.loc[best_match_index, 'primary_title'],
        'uid': row['uid']
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

#extract rows from embase_results_df that are not in combined_df
fuzzy_title_embase_df = embase_results_dedupe[
    (~embase_results_dedupe['doi_retrieved'].isin(final_df['doi_retrieved']) | pd.isna(embase_results_dedupe['doi_retrieved'])) &
    (~embase_results_dedupe['pmid_retrieved'].isin(final_df['pmid_retrieved']) | pd.isna(embase_results_dedupe['pmid_retrieved']))
]
matching_df_fuzzy = final_df[pd.isna(final_df['doi_sent']) & pd.isna(final_df['pmid_sent']) & ~pd.isna(final_df['included_reference'])]

# Find matches for matching_df_fuzzy from fuzzy_title_embase_df
embase_results_dedupe['match_string'] = embase_results_dedupe.apply(combine_columns, axis=1)
best_matches_df = matching_df_fuzzy.apply(lambda row: find_best_match(row, embase_results_dedupe), axis =1)
# Reset index to include the original index from matching_df_fuzzy
best_matches_df = best_matches_df.reset_index()
# Rename the 'index' column to 'original_index' for clarity
best_matches_df = best_matches_df.rename(columns={'index': 'original_index'})
best_matches_df = best_matches_df[best_matches_df['similarity_score'] > 55]

# Merge with embase_results_dedupe using best_match_index
merged_matches = best_matches_df.merge(
    embase_results_dedupe,
    left_on='best_match_index',
    right_index=True,
    how='left',
    suffixes=('', '_embase')
)
final_df_indexed = final_df.set_index('uid')
merged_matches_indexed = merged_matches.set_index('uid')
# Use combine_first to merge the new information into final_df
final_df_updated = final_df_indexed.combine_first(merged_matches_indexed)
# Reset the index to make 'uid' a column again
final_df_updated = final_df_updated.reset_index()

# Optionally, reorder columns to ensure original columns come first
original_columns = final_df.columns.tolist()
new_columns = [col for col in final_df_updated.columns if col not in original_columns]
final_df_updated = final_df_updated[original_columns + new_columns]

# Save the updated final_df
final_df_updated.to_csv('retrieval_results/embaseresults_ovidsearch_pmid_doi.csv', index=False)