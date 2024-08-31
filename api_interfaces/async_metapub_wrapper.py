from dotenv import load_dotenv
load_dotenv() 
from metapub import PubMedFetcher
from metapub.exceptions import MetaPubError, InvalidPMID
import pandas as pd 
import numpy as np 
import asyncio 
from concurrent.futures import ThreadPoolExecutor
import os 
from tqdm.asyncio import tqdm
import logging

class async_metapub_wrapper: 

    def __init__(self): 
        self.api_instance = PubMedFetcher()
        #api limits are set at rps 
        self.api_limit = 9
        self.pubmed_result_col = ['api_id_retrieved', 'title', 'abstract', 'publication_year', 'venue', 'doi', 'mesh_headings', 'authors', 'url', 'pmid']
        self.none_result_placeholder = (None,)*len(self.pubmed_result_col)

    async def async_fetch_pubmed_articles(self, df, title_search_flag):
        
        #preserve original index 
        df = df.reset_index(drop=False)


        with ThreadPoolExecutor(max_workers = self.api_limit) as executor:

            if title_search_flag: 
                id_col = 'title_sent'
            elif title_search_flag == False: 
                id_col = 'id_sent_apiretrieval'
            
            id_list = df[id_col].to_list()
            tasks = [self.task_executor(id, executor) for id in id_list]
            retrieval_results_list = await tqdm.gather(*tasks)

        pubmed_result_list = []
        #do processing 
        for pub in retrieval_results_list: 
            #handle results from title search 
            if title_search_flag:
                if isinstance(pub, list): 
                    pubmed_api_data = pub 
                else: 
                    pubmed_api_data = [pub]

            elif title_search_flag == False:
                if pub is not None: 
                    pubmed_api_data = (pub.pmid, pub.title, pub.abstract, pub.year, pub.journal, pub.doi, pub.mesh, pub.authors, pub.url, pub.pmid)
                else: 
                    pubmed_api_data = self.none_result_placeholder

            pubmed_result_list.append(pubmed_api_data)

        if title_search_flag: 
            #pubmed result list at this point is a nested list of pmids - convert to pd series 
            pubmed_result_df = pd.Series(pubmed_result_list, name = 'pmid_retrieved')
        elif title_search_flag == False: 
            pubmed_result_df = pd.DataFrame(pubmed_result_list, columns = self.pubmed_result_col)

        #merge back with original df 
        #concat to input df 
        retrieval_results = pd.concat([df, pubmed_result_df], axis=1)
        return retrieval_results 

    async def task_executor(self, id, executor): 
        #get loop 
        loop = asyncio.get_event_loop()

        #triage doi types to metapub 
        def sync_pubmed_calls():
            try: 
                if isinstance(id, str):  
                    if id.startswith('pmid:'):
                        return self.api_instance.article_by_pmid(id.strip('pmid:'))
                    elif id.startswith('10.'):
                        return self.api_instance.article_by_doi(id)
                    elif id.startswith('no_id_provided'):
                        return None
                    elif id is None: 
                        return None
                    else: 
                        query = f'title:"{id}"'
                        #this will return a list of pmids
                        try:  
                            return self.api_instance.pmids_for_query(query, retmax = 5)
                        except Exception as e: 
                            print(e)
                            #return empty list 
                            return []
                else: 
                    return None
            except InvalidPMID as e: 
                logging.error(f"Caught Invalid PMIDError: {str(e)}")
                if str(e).endswith('not found'): 
                    return None
            except MetaPubError as e: 
                error_msg = str(e)
                logging.error(f"Caught MetaPubError: {error_msg}") 
                if error_msg.startswith('No PMID available for doi'): 
                    return None
            except Exception as e: 
                logging.error(f"Unexpected error: {str(e)}")
                return None
        
        return await loop.run_in_executor(executor, sync_pubmed_calls)
        

