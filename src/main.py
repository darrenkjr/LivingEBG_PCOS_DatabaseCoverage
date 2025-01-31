from pmid_doi_search import main as run_api_search
from process_embase_results import main as process_embase_ris_results
from process_ovid_secondroundtitle import main as process_ovid_secondroundtitle_results


if __name__ == "__main__":
    run_api_search()
    process_embase_ris_results()
    process_ovid_secondroundtitle_results()

