import asyncio
import aiohttp 
import pandas as pd
from aiolimiter import AsyncLimiter
import platform 
import rispy 
import re
from numpy import nan
import logging 
import urllib.parse
from aiohttp.client_exceptions import ClientError
import random 
import numpy as np

#test semantic scholar API functionality
# 

class semanticscholar_interface: 

    def __init__(self,api_key): 
        
        self.batch_limit = 500
        self.concurrent_requests = 2
        self.semaphore = asyncio.Semaphore(self.concurrent_requests)
        self.api_limit = AsyncLimiter(3,1)
        self.session_timeout = aiohttp.ClientTimeout(total=900)
        self.pagination_limit = 500
        self.default_pagination_offset = 0
        self.max_retries = 20
        self.api_key = api_key
        self.error_log = []
        self.concurrent_requests = 5
        self.delay_between_batches = 1.0
        self.fields = 'title,abstract,externalIds,referenceCount,citationCount,year,publicationVenue,journal,publicationTypes,s2FieldsOfStudy'
        self.api_endpoint = 'https://api.semanticscholar.org/graph/v1/paper/{id}/{citation_direction}?offset={offset}&limit={limit}&fields={fields}'
        self.batch_endpoint = 'https://api.semanticscholar.org/graph/v1/paper/batch'
        self.generic_paper_endpoint = 'https://api.semanticscholar.org/graph/v1/paper/{id_type}:{id}?fields={fields}'
        self.logger = logging.getLogger(__name__)
        #set logger to warning and above

        if platform.system()=='Windows':
                asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    def generate_default_api_path(self,input_id,direction):
        
        self.logger.info('Generating default API path for id: %s and direction: %s',id,direction)
        ss_path_list = []
        no_id_path_list = []
        #check if input_id is a doi :
        if direction == 'generic':
            self.generic = True

            for k,v in input_id.items():
                if k == 'doi_list' and v:
                    for doi in v: 
                        paper_endpoint = self.generic_paper_endpoint.format(id_type = 'doi', id = doi, fields = self.fields)
                        ss_path_list.append(paper_endpoint)
                elif k == 'pmid_list' and v: 
                    for pmid in v: 
                        paper_endpoint = self.generic_paper_endpoint.format(id_type = '', id = pmid, fields = self.fields)
                        ss_path_list.append(paper_endpoint)
                elif k == 'ss_list' and v: 
                    for ss_id in v: 
                        paper_endpoint = self.generic_paper_endpoint.format(id_type = 'CorpusId', id = ss_id, fields = self.fields)
                        ss_path_list.append(paper_endpoint)
                elif k == 'noid_list' and v: 
                    print('No ID detected - not generating any API paths for these')
                        # paper_endpoint = self.generic_paper_endpoint.format(id_type = 'no_id', id = noid, fields = self.fields)
                        # ss_path_list.append(paper_endpoint)
                else: 
                    continue

        elif direction != 'generic':
            list_dict = []
            self.generic = False 
            for i in input_id:  
                if i is None or not i: 
                    self.logger.warn('ID is None, skipping')
                    return None 
                else: 
                    api_path = self.api_endpoint.format(id =i, citation_direction = direction, offset=self.default_pagination_offset,limit =self.pagination_limit, fields = self.fields)

                    api_path_dict = {
                        'id' : i,
                        'api_path' : api_path
                    }
                
                    list_dict.append(api_path_dict)

        if direction == 'generic':
            return ss_path_list
        elif direction != 'generic':
            return list_dict


    async def retrieve_generic_paper_details(self, df): 
        if 'seed_Id' in df.columns:
            df['id'] = df['seed_Id'].fillna(df['seed_pmid'].apply(lambda x: 'pmid:' + str(int(x)) if pd.notnull(x) else x))
        elif 'included_article_doi' in df.columns:
            #convert doi to lower case 

            df['included_article_doi'] = df['included_article_doi'].str.lower().str.replace(" ", "")
            #fill empty with pmid and mag id 
            df['id'] = df['ss_id'].apply(lambda x : str(int(x))if not pd.isna(x) else x)
            df['id'] = df['id'].fillna(
                df['included_article_pmid'].apply(lambda x: 'pmid:' + str(int(x)) if not pd.isna(x) else x)
            ).fillna(
                df['included_article_doi']
            )
            df['id'] = df.apply(lambda x : 'no_id_provided'if pd.isna(x['id']) else x['id'], axis=1)

        #remove all whitespace 
        df['id'] = df['id'].str.replace(r'\s+', '', regex=True)

        id_list = df['id'].tolist()
        id_dict = self.id_source_splitter(id_list)
        doi_list = ["doi:" + x for x in id_dict['doi_list']]
        pmid_list = id_dict['pmid_list']
        ss_list = ["CorpusId:" + x for x in id_dict['ss_list']] 
        id_flat_list = [item for sublist in [doi_list, pmid_list, ss_list] for item in sublist]

        #construct post payload 
        result = await self.batch_retrieve_details(id_flat_list) 

        #post processing 
        result['doi'] = result['externalIds'].apply(lambda x: (x.get('DOI')).lower() if isinstance(x, dict) and x.get('DOI') else None)
        result['pmid'] = result['externalIds'].apply(lambda x : 'pmid:' + x.get('PubMed') if isinstance(x, dict) and x.get('PubMed') else None)
        result['api_id_retrieved'] = result['externalIds'].apply(lambda x: str(int(x.get('CorpusId'))) if isinstance(x, dict) else None)

        #staged matching 
        df['original_index'] = df.index
        result = result.drop_duplicates(subset = ['api_id_retrieved'])
        #perfor merging 
        final_df = self.merge_with_original_df(df, result)
        final_df['citation_network_size'] = final_df.apply(
            lambda row: row['citationCount'] + row['referenceCount'] 
            if pd.notna(row['citationCount']) and pd.notna(row['referenceCount']) 
            else row['citationCount'] if pd.notna(row['citationCount']) 
            else row['referenceCount'], 
            axis=1)
        
        return final_df

    def merge_with_original_df(self, df, result): 

        print('Performing merging based on doi')
        merged_df = df.merge(result, left_on='id', right_on='doi', how='left', suffixes=('', '_doimatch'))

        # Step 2: Merge based on pmid, keeping existing columns where present
        print('Performing merging based on pmid')
        merged_df = merged_df.merge(result, left_on='id', right_on='pmid', how='left', suffixes=('', '_pmidmatch'))
        for col in result.columns:
            if col not in ['doi', 'pmid', 'api_id_retrieved']:
                merged_df[col] = merged_df[col].combine_first(merged_df[f'{col}_pmidmatch'])

        # Step 3: Merge based on oa_id_retrieved, keeping existing columns where present
        print('Performing merging based on ss id ')
        merged_df = merged_df.merge(result, left_on='id', right_on='api_id_retrieved', how='left', suffixes=('', '_apiidmatch'))
        for col in result.columns:
            if col not in ['doi', 'pmid', 'api_id_retrieved']:
                merged_df[col] = merged_df[col].combine_first(merged_df[f'{col}_apiidmatch'])

        # Optional: Drop the temporary matching columns if needed
        temp_suffixes = ['_pmidmatch', '_apiidmatch']
        for suffix in temp_suffixes:
            for col in result.columns:
                if col not in ['doi', 'pmid', 'api_id_retrieved']:
                    merged_df = merged_df.drop(columns=[f'{col}{suffix}'])

        # Keep the original id columns for checking later
        # Optionally, rename these columns if desired
        merged_df = merged_df.rename(columns={
            'doi_doimatch': 'doi_match',
            'pmid_pmidmatch': 'pmid_match',
            'api_id_retrieved_apiidmatch': 'api_id_match'
        })

        #reinsert no_id_provided rows in order 
        merged_df.set_index('original_index', inplace=True)
        # Concatenate the rows where id was "no_id_provided" back in their original order
        final_df = merged_df.copy()
        final_df['included_article_doi'] = final_df['doi'].fillna(final_df['doi_pmidmatch']).fillna(final_df['doi_apiidmatch'])
        final_df['inluded_article_pmid'] = final_df['pmid'].fillna(final_df['pmid_match']).fillna(final_df['pmid_apiidmatch'])
        final_df['api_id_retrieved'] = final_df['api_id_retrieved'].fillna(final_df['api_id_retrieved']).fillna(final_df['api_id_retrieved_pmidmatch']).fillna(final_df['api_id_match'])
        final_df.rename(columns = {'id' : 'id_sent_apiretrieval'}, inplace= True)

        final_df.drop(columns = ['doi_pmidmatch', 'doi_apiidmatch', 'pmid_apiidmatch', 'pmid_match', 'api_id_retrieved_pmidmatch'], inplace = True)

        return final_df


    async def batch_retrieve_details(self, id_list): 
        result = []
        for i in range(0, len(id_list), self.batch_limit): 
            batch = id_list[i:i+self.batch_limit]
            async with aiohttp.ClientSession() as session: 
                async with session.post(self.batch_endpoint, json = {'ids': batch}, params = {'fields': self.fields}, headers = {'x-api-key':self.api_key}) as response:
                    if response.status == 200:  
                        response_data = await response.json()
                        if len(response_data) != len(batch):
                            print(f"Warning: Mismatch in the number of IDs sent and received. Sent: {len(batch)}, Received: {len(response_data)}, batch_index: {i} to {self.batch_limit}")
                        for j, data in enumerate(response_data): 
                            if data is not None: 
                                data['batch_ids'] = batch
                                result.append(data)
                            else: 
                                print(f'No data found for this batch, specifically at position {j} within current batch')
                                data = {'paperId': None }
                                result.append(data)

                    else: 
                        print(f"API Call failed. Error code: {response.status}")
                        
        result_df = pd.DataFrame(result)
        return result_df    
    


        
    async def retrieve_paper_details(self, api_path_dict):
        result = {}
        api_path, current_seed_id = self._prepare_api_call(api_path_dict)
        
        async with aiohttp.ClientSession(timeout=self.session_timeout) as session:
            async with self.semaphore: 
                if self._is_no_id(api_path):
                    return None
                else:
                    ss_results_json = await self._retry_api_call(session, api_path)
                    processed_results = await self._process_or_dummy_results(ss_results_json, api_path,session)
                
                if self.generic:
                    processed_results.rename(columns={'paperId': 'paper_Id'}, inplace=True)
                    return processed_results
                else:
                    if processed_results.empty: 
                        processed_results = pd.DataFrame(columns = ['id','title','abstract','year','api_path'])
                        processed_results['api_path'] = api_path
                        processed_results['no_data_from_api'] = 1
                    result = {
                        'id': current_seed_id,
                        'api_path': api_path,
                        'results': processed_results
                    }

                    #perfor empty check for processed_results 
                    if processed_results.empty:
                        print('No data found for api path: {}'.format(api_path))
                    return result

    def _prepare_api_call(self, api_path_dict):
        if self.generic:
            return api_path_dict, None
        else:
            return api_path_dict['api_path'], api_path_dict['id']

    def _is_no_id(self, api_path):
        return api_path.startswith('https://partner.semanticscholar.org/graph/v1/paper/no_id') and self.generic

    async def _process_or_dummy_results(self, ss_results_json, api_path, session):
        if ss_results_json is None:
            self.logger.error(f"Failed to retrieve data for api path {api_path}")
            if self.generic is True:
                dummy_data = {
                    'no_data_from_api': 1,
                    'originating_api_path': api_path
                }
                return pd.DataFrame(dummy_data, index=[0])
            elif self.direction == 'citations' or self.direction == 'references':
                dummy_df = pd.DataFrame(columns = ['id','title','abstract','year','api_path'])
                dummy_df['api_path'] = api_path
                return dummy_df 
        else:
            return await self._process_results(ss_results_json, api_path,session)
    
    async def _retry_api_call(self, session, api_path):
        retries = 0
        while retries <= self.max_retries: 
            try: 
                async with self.api_limit: 
                    print(f'Requesting data for {api_path}')
                    async with session.get(api_path, headers = {'x-api-key':self.api_key}) as resp:
                        if resp.status == 200: 
                            print('API call succeeded')
                            return await resp.json() 
                        elif resp.status == 429: 
                            retry_after = int(resp.headers.get("Retry-After", 1))  # Get retry-after header
                            self.logger.warning(f'API limit reached for path: {api_path}, retrying after {retry_after} seconds')
                            self.logger.warning('API limit reached, backing off')
                        elif resp.status == 504: 
                            self.logger.warning('Server-side timeout, backing off')
                        elif resp.status == 404: 
                            self.logger.warning(f'404 Error for following api path: {api_path}, probably due to no data found.')
                            error_text = await resp.json()
                            return None
                        else: 
                            self.logger.warning(f'API call failed with status code: {resp.status}, path: {api_path}, backing off')
            except ClientError as e:
                self.logger.error(f'Client side error: {e}')

            retries +=1 
            if retries > self.max_retries: 
                self.logger.error(f'Failed to retrieve data for api_path {api_path} after retries')
                return None
                    # Exponential backoff with jitter
            backoff_time = (2 ** retries) + random.uniform(0, 1)
            await asyncio.sleep(backoff_time)

    async def _process_results(self,json,api_path,session):
        if self.generic is True: 
            initial_result_df = pd.json_normalize(json)
        else: 
            initial_result_df = pd.json_normalize(json,record_path=['data'])
        data_len = len(initial_result_df)
        if data_len >= self.pagination_limit: 
            combined_data = await self._handle_pagination(initial_result_df,session, api_path)
            #pagination data should be a list of dataframes 
            full_result_df = pd.concat(combined_data, ignore_index=True)
        else: 
            #no pagination required 
            full_result_df = initial_result_df
        
        if self.generic is not True:
            if self.direction == 'citations': 
                full_result_df['reference_or_citation'] = 'citation'
                full_result_df.columns = full_result_df.columns.str.replace('citingPaper.', '',regex=True)
            elif self.direction == 'references':
                full_result_df['reference_or_citation'] = 'reference'
                full_result_df.columns = full_result_df.columns.str.replace('citedPaper.', '',regex=True)
            full_result_df.columns = full_result_df.columns.str.replace('externalIds.', '',regex=True)
            full_result_df.rename(columns = {
                'paperId' : 'paper_Id', 
            }, inplace=True)

        full_result_df['originating_api_path'] = api_path

        doi_pattern = r"(?<=paper\/)([\w\d\./]+)(?:\/references|\/citations)"
        pmid_pattern = r"pmid:(\d+)(?:\/(?:references|citations))?"
        pattern_list = [doi_pattern,pmid_pattern]
        originating_id = next((match.group(1) for pattern in pattern_list for match in re.finditer(pattern, api_path)), None)
        full_result_df['originating_seed_id'] = originating_id

        return full_result_df

    async def _handle_pagination(self,initial_data, session,api_path):

        combined_data = [initial_data]
        pagination_offset = self.default_pagination_offset

        while True: 
            pagination_offset += self.pagination_limit
            new_api_path = re.sub(r"(?<=offset=)(.*)(?=&limit)",str(pagination_offset),api_path)

            pagination_json_data = await self._retry_api_call(session, new_api_path)
            paginated_df = pd.json_normalize(pagination_json_data,record_path=['data'])

            if not paginated_df.empty and len(paginated_df) >= self.pagination_limit: 
                combined_data.append(paginated_df)
            else: 
                if not paginated_df.empty: 
                    combined_data.append(paginated_df)
                break 
        return combined_data


    async def retrieve_citations(self, article_df): 
        '''retrieves citation data from a given article dataframe'''
        self.direction = 'citations'
        forward_snowball_tasks = []

        article_df['seed_Id'].fillna('pmid:'+ article_df['seed_pmid'], inplace=True)

        if type(article_df['seed_Id']) == str:
            id_list = [article_df['seed_Id']]
        else: 
            id_list = article_df['seed_Id'].tolist()
    
        api_path_list = self.generate_default_api_path(id_list,'citations')
        for api_dict in api_path_list:
            forward_snowball_tasks.append(self.retrieve_paper_details(api_dict))
        ss_results_citations = await asyncio.gather(*forward_snowball_tasks)
        print('Citation retrieval done')
        #returns list of dataframes 
        return ss_results_citations 
    
    async def retrieve_references(self, article_df): 
        '''retrieves reference data from a given article dataframe'''
        self.direction = 'references'
        backward_snowball_tasks = []
        ss_results_references = pd.DataFrame()
        #fill empty spots with seed pmid 
        article_df['seed_Id'].fillna('pmid:'+ article_df['seed_pmid'], inplace=True) 

        if type(article_df['seed_Id']) == str:
            id_list = [article_df['seed_Id']]
        else: 
            id_list = article_df['seed_Id'].tolist()
            
        api_path_list = self.generate_default_api_path(id_list,'references')

        for api_dict in api_path_list: 
            backward_snowball_tasks.append(self.retrieve_paper_details(api_dict))
        ss_results_references = await asyncio.gather(*backward_snowball_tasks)
        # ss_consolidated_references = pd.concat(ss_results,ignore_index=True)
        print('reference retrieval done')
        return ss_results_references
    
    async def _retrieve_generic_paper_details(self,df): 
        '''
        deprecated
        '''
        generic_retrieval_tasks = [] 
        if 'seed_Id' in df.columns:
            df['id'] = df['seed_Id'].fillna(df['seed_pmid'].apply(lambda x: 'pmid:' + str(int(x)) if pd.notnull(x) else x))

        elif 'included_article_doi' in df.columns:
            #convert doi to lower case 
            df['included_article_doi'] = df['included_article_doi'].str.lower()
            #fill empty with pmid and mag id 
            df['id'] = df['included_article_doi'].fillna(
                df['included_article_pmid'].apply(lambda x: 'pmid:' + str(int(x)) if not pd.isna(x) else x)
            ).fillna(
                df['ss_id'].apply(lambda x : str(int(x))if not pd.isna(x) else x)
            )
            df['id'] = df.apply(lambda x : 'no_id_provided'if pd.isna(x['id']) else x['id'], axis=1)
        
        id_list = df['id'].tolist()
        id_dict = self.id_source_splitter(id_list)
        api_path = self.generate_default_api_path(id_dict,"generic")
        self.logger.info('Finished generating API paths for generic paper details')


        generic_retrieval_tasks = [self.retrieve_paper_details(url) for url in api_path]
    
        self.logger.info('Awaiting generic paper details retrieval tasks to complete')
        ss_results_list = await self.task_batching(
            tasks = generic_retrieval_tasks, 
            batch_size = self.concurrent_requests, 
            delay_between_batches = self.delay_between_batches
        )
        ss_results = pd.concat(ss_results_list, ignore_index= True)

        #identifies that current operation is either retrieving included or seed article details 
        ss_results['input_id'] = ss_results.apply(self.extract_id, axis = 1)
        df['encoded_id'] = df['id'].apply(lambda x : urllib.parse.quote(x))
        if 'included_doi' in df.columns: 
            target_col_merge = ['original_sr_id','id','encoded_id','ref_if_no_id', 'not_retrieved']
        else: 
            col_exclude = ['citations','references']
            target_col_merge = df.columns.difference(col_exclude).tolist()
        
        ss_results_merge = pd.merge(df[target_col_merge], ss_results, left_on = 'encoded_id', right_on='input_id', how = 'left', suffixes = ('_input', '_ssresp'))
        ss_results_merge.drop(columns = ['encoded_id'], inplace = True)
        rename_dct = {} 
        drop_col = []
        for col in ss_results_merge.columns: 
            if col.endswith('_ssresp'):
                api_col_name = col.replace('_ssresp','')
                rename_dct[col] = api_col_name 
                drop_col.append(api_col_name + '_input')
        ss_results_merge.rename(columns = rename_dct, inplace= True)
        ss_results_merge.drop(columns = drop_col, inplace = True)

        if 'no_data_from_api' not in ss_results_merge.columns:
            ss_results_merge['no_data_from_api'] = 0
        else: 
            ss_results_merge['no_data_from_api'].fillna(0, inplace = True)

        ss_results_merge['title'] = ss_results_merge['title'].fillna('No title found')
        ss_results_merge['abstract'] = ss_results_merge['abstract'].fillna('No abstract found')

        ss_results = ss_results_merge.copy()

        return ss_results

    def extract_id(self, row):
        match = re.search(r'(?:doi|pmid|mag):([^?]+)', row['originating_api_path'])
        if match:
            return match.group(1)
        else:
            return np.NaN 
    
    def id_source_splitter(self,id_list): 
        '''Checks id list for mixed formats, such as DOI vs PMID and splits it into separate lists'''

        pmid_list = []
        doi_list = []
        ss_list = []
        nan_list = []

        for item in id_list:
            if item.startswith('no_id'): 
                self.logger.warning('no_id found in id list - probably due to unobtainable DOI / PMID or MAG during data extraction')
                nan_list.append(item)
            elif item.startswith('pmid'):
                pmid_list.append(item)
            elif item.startswith('10.'):
                doi_list.append(item)
            else: 
                ss_list.append(item)

        # doi_list_encoded = [urllib.parse.quote(doi) for doi in doi_list]

        #only return lists that are not empty 
        list_dict = {
            'doi_list' : doi_list,
            'pmid_list' : pmid_list,
            'ss_list' : ss_list,
            'noid_list' : nan_list
        }

        return list_dict
    
    async def task_batching(self, tasks, batch_size, delay_between_batches): 

        async def sem_task(task): 
            async with self.semaphore: 
                return await task
        results = []
        for i in range(0, len(tasks), batch_size):
            batch = tasks[i:i + batch_size]
            batch_results = await asyncio.gather(*[sem_task(task) for task in batch]) 
            results.extend(batch_results)
            if i + batch_size < len(tasks):  # Add delay only if there are more batches
                await asyncio.sleep(delay_between_batches)
        return results

    def to_ris(self,df): 

        result_df_ss = df 
        entries = result_df_ss.copy() 
        entries['database_provider'] = 'Semantic Scholar'
        entries.rename(columns ={
            'paper_Id':'id',
            'paper_Title':'title',
            'paper_Abstract':'abstract',
            'paper_Venue':'journal_name',
            'paper_Year':'year',
            'paper_author':'authors',
        }, inplace=True)

        #unpack author column to get list of authors (nested dictionary)
        author_data = pd.json_normalize(entries['authors'].apply(lambda x : eval(x)))
        author_data = author_data.applymap(lambda x: {} if pd.isnull(x) else x)
        colname_range = range(1, len(list(author_data))+1)
        new_cols = ['A' + str(i) for i in colname_range]
        author_data.columns = new_cols
        author_names = author_data.apply(lambda x : x.str.get('name'), axis = 1)
        author_names = author_names.apply(lambda x : list(x.tolist()), axis = 1)
        author_names = author_names.apply( lambda x : list(filter(lambda item: item is not None, x)))
        author_names.name = 'authors'
        entries = pd.concat([entries, author_names], axis = 1)
        entries_ris = entries.to_dict('records')
        ris_export_path = 'result.ris'
        with open (ris_export_path, 'w', encoding = 'utf-8') as f: 
            rispy.dump(entries_ris,f)

        

