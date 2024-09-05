import pandas as pd
import unicodedata
from embase_process_ris import preprocess_text
from api_interfaces.embase_search_generator import generate_title_search_files

folder = 'title_searches/'
embase = pd.read_csv(folder + 'title_search_embase_fixed.csv')
pubmed = pd.read_csv(folder + 'title_search_pubmed_fixed.csv')

embase['title'] = embase['title'].apply(lambda x : preprocess_text(x))
pubmed['title'] = pubmed['title'].apply(lambda x : preprocess_text(x))

generate_title_search_files(embase, folder, 'embase')
generate_title_search_files(pubmed, folder, 'pubmed')