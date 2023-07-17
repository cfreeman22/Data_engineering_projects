from job_board.etl.extract import Extract
from job_board.etl.transform import Transform
import datetime as dt
import pandas as pd
import os 


def test_extract_transform_integration():

    '''
    Tests the extract and transform integration
    '''   
    # assemble 
    api_key_id = os.environ.get("api_key_id")

    request_id, df = Extract.extract(job_title="Data Engineer", api_key_id=api_key_id)
    df_region_codes = Extract.extract_region_codes("job_board/data/usa_regions.csv")

    # act 
    df_transformed = Transform.transform(df=df, df_regions=df_region_codes, request_id=request_id)

    # assert 

    df_transformed["extract_date"] = pd.to_datetime(dict(year=df_transformed.job_year, month=df_transformed.job_month, day=df_transformed.job_day)).apply(lambda x: x.date())
    extract_date = df_transformed["extract_date"].max()
    
    current_date = dt.datetime.now(dt.timezone.utc).date()

    print("request_id check: ", request_id)
    print("df_transformed column check:", df_transformed.columns)

    print("Test truth: ",(extract_date == current_date))

    assert extract_date == current_date

if __name__ == "__main__": 
    
    test_extract_transform_integration()