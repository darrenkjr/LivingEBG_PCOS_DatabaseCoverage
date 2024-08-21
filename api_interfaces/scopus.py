import asyncio
from typing import Dict, Any
import pandas as pd
from pybliometrics.scopus import AbstractRetrieval
from pybliometrics.scopus.exception import Scopus429Error, ScopusQueryError
from aiolimiter import AsyncLimiter

class ScopusInterface:
    def __init__(self, rate_limit: int = 5, time_period: int = 1):
        # Rate limit: maximum number of requests per time period (in seconds)
        self.limiter = AsyncLimiter(rate_limit, time_period)

    async def process_article(self, article_id: str) -> Dict[str, Any]:
        async with self.limiter:
            try:
                # Try to retrieve article details using DOI first, then PubMed ID
                for id_type in ['doi', 'pubmed_id']:
                    try:
                        abstract = AbstractRetrieval(article_id, id_type=id_type, refresh=True)
                        break
                    except ValueError:
                        continue
                else:
                    # If both attempts fail, return None
                    print(f"Unable to retrieve article with ID: {article_id}")
                    return None

                return {
                    'api_id_retrieved': abstract.eid,
                    'title': abstract.title,
                    'authors': [author.indexed_name for author in abstract.authors] if abstract.authors else None,
                    'publication_year': abstract.coverDate.year if abstract.coverDate else None,
                    'journal': abstract.publicationName,
                    'doi': abstract.doi,
                    'citation_network_size': abstract.citedby_count,
                    'abstract': abstract.abstract,
                    # Add more fields as needed
                }
            except (ScopusQueryError, Scopus429Error) as e:
                print(f"Error processing article {article_id}: {str(e)}")
                return None

    async def retrieve_generic_paper_details(self, df: pd.DataFrame) -> pd.DataFrame:
        tasks = []
        for _, row in df.iterrows():
            article_id = row['id_sent_apiretrieval']
            if pd.notna(article_id):
                tasks.append(self.process_article(article_id))

        results = await asyncio.gather(*tasks)

        # Process results and update the DataFrame
        for i, result in enumerate(results):
            if result:
                for key, value in result.items():
                    df.at[i, key] = value

        return df

# Usage example:
# scopus_interface = ScopusInterface(rate_limit=5, time_period=1)
# result_df = asyncio.run(scopus_interface.retrieve_generic_paper_details(input_df))