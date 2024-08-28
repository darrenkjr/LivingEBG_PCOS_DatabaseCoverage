import asyncio
from typing import Dict, Any, List, Tuple
import pandas as pd
from pybliometrics.scopus import AbstractRetrieval
from pybliometrics.scopus.exception import Scopus429Error, ScopusQueryError, Scopus401Error
import pybliometrics 
from aiolimiter import AsyncLimiter
from tqdm import tqdm

class scopus_interface:
    def __init__(self, rate_limit: int = 5, time_period: int = 1):
        pybliometrics.scopus.init()
        
        self.limiter = AsyncLimiter(rate_limit, time_period)

    async def process_article(self, article_id: str, id_type: str) -> Dict[str, Any]:
        async with self.limiter:
            try:
                print(f"Attempting to retrieve article with {id_type}: {article_id}")
                abstract = AbstractRetrieval(article_id, id_type=id_type, refresh=True, view = 'META')
                print(f"Successfully retrieved article with {id_type}: {article_id}")

                return {
                    'api_id_retrieved': abstract.eid,
                    'title': abstract.title,
                    'authors': [author.indexed_name for author in abstract.authors] if abstract.authors else None,
                    'journal': abstract.publicationName,
                    'doi': abstract.doi,
                    'citations_count': abstract.citedby_count,
                    'references' : abstract.references if abstract.references else None,
                    'references_count' : abstract.refcount if abstract.refcount else None,
                    'abstract': abstract.abstract,
                    'publication_year': abstract.coverDate if abstract.coverDate else None,
                    'pmid': abstract.pubmed_id if abstract.pubmed_id else None,
                    # Add more fields as needed
                }
            except (ScopusQueryError, Scopus429Error, Scopus401Error) as e:
                error_url = getattr(e, 'url', 'URL not available')
                print(f"Error processing article {article_id}: {str(e)}")
                print(f"Problematic URL: {error_url}")
                print(f"Error type: {type(e).__name__}")
                return None
            except Exception as e:
                print(f"Unexpected error for article {article_id}: {str(e)}")
                print(f"Error type: {type(e).__name__}")
                return None

    def sort_article_ids(self, df: pd.DataFrame) -> Tuple[List[str], List[str]]:
        dois = df[df['id_sent_apiretrieval'].str.contains('10.', na=False)]['id_sent_apiretrieval'].tolist()
        #pmids will contain pmid: 
        pmids = df[df['id_sent_apiretrieval'].str.contains('pmid:', na=False)]['id_sent_apiretrieval'].tolist()
        return dois, pmids

    async def retrieve_generic_paper_details(self, df: pd.DataFrame) -> pd.DataFrame:
        # Create or update 'id_sent_apiretrieval' column

        print(f"Total articles to process: {len(df)}")
        print("Starting article retrieval process...")

        dois, pmids = self.sort_article_ids(df)
        print(f"Number of DOIs: {len(dois)}")
        print(f"Number of PubMed IDs: {len(pmids)}")

        tasks = []
        for doi in dois:
            tasks.append(self.process_article(doi, 'doi'))
        for pmid in pmids:
            #strip leading pmid: 
            pmid = pmid.replace('pmid:', '')
            tasks.append(self.process_article(pmid, 'pubmed_id'))

        results = []
        for f in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Processing articles"):
            try:
                result = await f
                results.append(result)
                if result:
                    print(f"Retrieved article: {result.get('title', 'Unknown Title')[:50]}...")
                else:
                    print("Failed to retrieve an article")
            except Exception as e:
                print(f"Unexpected error during article retrieval: {str(e)}")
                results.append(None)

        print("Article retrieval process completed.")
        print(f"Successfully retrieved {sum(1 for r in results if r is not None)} out of {len(tasks)} articles.")

        # Process results and update the DataFrame
        for result in results:
            #dummy matching row 
            matching_row = []
            if result:
                matching_row = df[df['id_sent_apiretrieval'] == result['doi']].index
                if len(matching_row) == 0 and result.get('pmid'):
                    matching_row = df[df['id_sent_apiretrieval'] == f"pmid:{result['pmid']}"].index
                if len(matching_row) > 0:
                    for key, value in result.items():
                        df.at[matching_row[0], key] = value

        return df

    async def test_api_connection(self):
        test_doi = "10.1016/j.softx.2019.100263"  # A known DOI for testing
        try:
            result = await self.process_article(test_doi, 'doi')
            if result:
                print("API connection test successful")
                print(f"Retrieved title: {result['title']}")
            else:
                print("API connection test failed: No result returned")
        except Exception as e:
            print(f"API connection test failed: {str(e)}")

# Usage example:
if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    try:
        print("Initializing Scopus interface...")
        scopus_interface = ScopusInterface(rate_limit=5, time_period=1)
        
        print("Testing API connection...")
        asyncio.run(scopus_interface.test_api_connection())
        
        print("Reading input Excel file...")
        excel_path = '../PCOS_Guideline_Dataset.xlsm'
        input_df = pd.read_excel(excel_path, sheet_name="included_articles")
        logger.info(f"Input DataFrame shape: {input_df.shape}")
        logger.info(f"Input DataFrame columns: {input_df.columns.tolist()}")
        
        # Log the number of DOIs and PMIDs before processing
        doi_count = input_df['included_article_doi'].notna().sum()
        pmid_count = input_df['included_article_pmid'].notna().sum()
        print(f"Number of DOIs in input: {doi_count}")
        print(f"Number of PMIDs in input: {pmid_count}")
        
        print("Starting retrieval process...")
        result_df = asyncio.run(scopus_interface.retrieve_generic_paper_details(input_df))
        
        logger.info(f"Result DataFrame shape: {result_df.shape}")
        logger.info(f"Result DataFrame columns: {result_df.columns.tolist()}")
        
        # Log the number of processed articles
        processed_count = result_df['api_id_retrieved'].notna().sum()
        print(f"Number of successfully processed articles: {processed_count}")
        
        print("Sample of retrieved data:")
        print(result_df[['id_sent_apiretrieval', 'api_id_retrieved', 'title']].head())
        
        # Save the results to a new Excel file
        output_path = 'scopus_retrieval_results.xlsx'
        result_df.to_excel(output_path, index=False)
        print(f"Results saved to {output_path}")
        
        print("Process completed successfully.")
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        print(f"An error occurred: {str(e)}")