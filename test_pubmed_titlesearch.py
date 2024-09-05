#test pubmed api response when conducting a title search 
import metapub

#just do 1 call 
pubmed_instance = metapub.PubMedFetcher()
id = "diagnosis of polycystic ovaries by three-dimensional transvaginal ultrasound"
query = f"ti:\"{id}\""
# print(query)
# result = pubmed_instance.pmids_for_query(query, retmax = 5)
# print(result)

#test retrieval by citation details 
j_title = 'Fertility and Sterility' 
year = '2006'
volume = '85'
page = '214'


pmid = pubmed_instance.pmids_for_citation(jtitle = j_title, year = year, volume = volume, spage = page )
print(pmid)
details = pubmed_instance.article_by_pmid('35538531')
print(details)