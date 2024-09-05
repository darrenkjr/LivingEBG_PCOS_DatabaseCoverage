import pandas as pd
import os
import re
import numpy as np

def process_pmid(pmid):
    # Remove 'pmid:' prefix and any leading/trailing whitespace
    return pmid.replace('pmid:', '').strip()

def process_doi(doi):
    # Replace all punctuation with spaces
    return re.sub(r'[^\w\s]', ' ', doi)

def id_search_generator(pmids, dois):
    pmid_search = ' OR '.join(f'"{process_pmid(pmid)}".pm' for pmid in pmids if pd.notna(pmid))
    doi_search = ' OR '.join(f'"{process_doi(doi)}".do' for doi in dois if pd.notna(doi))
    
    if pmid_search and doi_search:
        return f'({pmid_search}) OR ({doi_search})'
    elif pmid_search:
        return f'({pmid_search})'
    elif doi_search:
        return f'({doi_search})'
    else:
        return ''

def extract_gdg(question_id):
    return int(str(question_id).split('.')[0])

def sanitize_filename(filename):
    return re.sub(r'[<>:"/\\|?*]', '_', filename)

def generate_id_search_files(oa_results, base_output_dir, database):
    # Initialize search_ids as an empty list
    search_ids_list = []

    # Process each question_id group
    for question_id, group in oa_results.groupby('question_id'):
        gdg = extract_gdg(question_id)
        gdg_dir = os.path.join(base_output_dir, f'GDG_{gdg}')
        os.makedirs(gdg_dir, exist_ok=True)

        # Get non-empty ids
        non_empty_ids = group[['pmid', 'doi']].dropna(how='all')
        
        article_count = len(group)
        
        id_search = id_search_generator(non_empty_ids['pmid'], non_empty_ids['doi'])
        
        # Create a text file for this question_id in the GDG-specific folder
        safe_question_id = sanitize_filename(str(question_id))
        filename = f'questionid_{safe_question_id}_{database}_idsearch.txt'
        file_path = os.path.join(gdg_dir, filename)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(f"GDG: {gdg}\n")
            f.write(f"questionid: {question_id}\n")
            f.write(f"Total Article Count: {article_count}\n")
            f.write("ID Search:\n")
            f.write(id_search)

        # Add question_id and GDG to non_empty_ids
        non_empty_ids['question_id'] = question_id
        non_empty_ids['GDG'] = gdg
        non_empty_ids['original_index'] = non_empty_ids.index
        non_empty_ids['included_article_id'] = group['included_article_id']
        non_empty_ids.rename(columns={'pmid': 'pmid_sent', 'doi': 'doi_sent'}, inplace=True)

        # Append to the list instead of concatenating
        search_ids_list.append(non_empty_ids)

    # Combine all DataFrames in the list
    search_ids = pd.concat(search_ids_list, ignore_index=True)

    return search_ids

def generate_title_search_files(input_df, base_output_dir, database):
    # Initialize search_ids as an empty list
    #go through title columns and create a search for everything else 
    title_search = ' OR '.join(f'"{title}".TI' for title in input_df['title'] if pd.notna(title))
    #write to file 
    with open(os.path.join(base_output_dir, f'{database}_title_search.txt'), 'w', encoding='utf-8') as f:
        f.write(title_search)


if __name__ == "__main__" : 
    # Get the directory of the current script
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # Create absolute paths
    excel_file_path = os.path.join(current_dir, '..', 'retrieval_results', 'api_retrieved.xlsx')
    output_dir = os.path.join(current_dir, '..', 'gdg_embase_searches')
    os.makedirs(output_dir, exist_ok=True)

    # Read in excel file
    oa_results = pd.read_excel(excel_file_path, sheet_name='api_results_oa')

    # Process each question_id group
    ids_sent  = generate_id_search_files(oa_results, output_dir, 'embase')
    #oa_results, first 15 columns 
    embase_ids_sent = oa_results.iloc[:, :15]
    embase_ids_sent['title'] = oa_results['title']
    embase_ids_sent['original_index'] = embase_ids_sent.index
    embase_ids_sent = pd.merge(embase_ids_sent, ids_sent, on=['GDG','question_id','included_article_id','original_index'], how='left')
    #drop the original_index column
    embase_ids_sent.drop(columns=['original_index'], inplace=True)
    embase_ids_sent.to_csv(os.path.join(output_dir, 'embase_ids_sent.csv'), index=False)
    #create separate search file for title only searches 
    title_only_searches = embase_ids_sent[embase_ids_sent['pmid_sent'].isna() & ~embase_ids_sent['doi_sent'].isna()]
    generate_title_search_files(title_only_searches, output_dir, 'embase')

    print(f"ID search files have been organized into GDG-specific folders within the '{output_dir}' directory.")