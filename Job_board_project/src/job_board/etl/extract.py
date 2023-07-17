import pandas as pd 
import requests

class Extract():
    '''
    Extract Class in the Job Board ETL Pipeline
    '''

    @staticmethod
    def extract(
            job_title:str, 
            api_key_id:str, 
            num_pages: str = 20,
            job_posting_date: str = "today"
        )->pd.DataFrame:
        """
        Extract Job Openings Data from the JSearch API (Rapid API). 
        JSearch API (Rapid API) Search for jobs posted on LinkedIn, Indeed, Glassdoor, ZipRecruiter, BeBee and many others, all in a single API
        - job_title: a string identifying job title e.g. data analyst, data engineer etc 
        - api_key_id: api key id from JSearch API (Rapid API)
        
        Returns: 
        - DataFrame with Job Posting data for the requested job_posting_date 
        """
        
        rapid_api_host = "jsearch.p.rapidapi.com"
        base_url = f"https://{rapid_api_host}/search"

        headers = {
                "X-RapidAPI-Key": api_key_id,
                "X-RapidAPI-Host": rapid_api_host
            }
        
        querystring = {"query": f"{job_title} in USA", "num_pages": num_pages, "date_posted":job_posting_date}

        response_data = []
            
        response = requests.get(base_url, params=querystring, headers=headers)
        if response.json().get("data") is not None: 
            response_data.extend(response.json().get("data"))
            request_id = response.json().get("request_id")
        # read json data to a dataframe 
        df = pd.json_normalize(data=response_data, meta=["symbol"])
        return request_id, df
    
    @staticmethod
    def extract_region_codes(fp:str)->pd.DataFrame:
        """
        Reads usa region codes CSV file and returns a dataframe.
        - fp: filepath to the exchange codes CSV file
        """
        df = pd.read_csv(fp)
        return df
