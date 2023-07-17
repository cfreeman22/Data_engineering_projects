from job_board.etl.transform import Transform
from job_board.etl.load import Load
import pandas as pd
import os 
import shutil


def test_transform():

    '''
    Tests the transform step
    '''
    # assemble 

    request_id_input = "9ad8865c-462e-4f0a-9401-118f843993da"

    df_input = pd.DataFrame({
        "employer_name": ["SDG Group USA", "Staffigo Technical Services, LLC", "Bloomberg"],
        "employer_website": ["http://www.sdggroup.com", "http://www.staffigo.com", "http://www.bloomberg.com"],
        "job_id": ["j0tMZs6Zk4MAAAAAAAAAAA==", "HoRqzMmvw8EAAAAAAAAAAA==" ,"c60z-dt44zEAAAAAAAAAAA=="],
        "job_employment_type": ["FULLTIME", "CONTRACTOR", "TEMPORARY"],
        "job_title": ["Data Analyst", "Data Engineer", "Data Scientist"],
        "job_description": ["Come join us! We need a Python expert", "Join our team for a SQL job", "Need an expert in cloud computing"],
        "job_is_remote": [True, False, True],
        "job_posted_at_datetime_utc": ["2023-02-10T13:29:06.000Z", "2022-06-02T13:29:06.000Z", "2021-11-15T13:29:06.000Z"],
        "job_city": ["Bedminster", "Bentonville", "Delano"],
        "job_state": ["NJ", "AR", "CA"],
        "job_country": ["US", "US", "US"],
        "job_benefits": [['dental_coverage', 'paid_time_off', 'health_insurance', 'retirement_savings'],
                        ['dental_coverage', 'health_insurance', 'retirement_savings'], 
                        ['dental_coverage', 'retirement_savings']],
        "job_min_salary": [50000, 100000, 150000],
        "job_max_salary": [55000, 105000, 155000],
        "job_required_experience.required_experience_in_months": [12, 48, 60],
        "job_required_education.postgraduate_degree": [True, False, False],
        "job_required_education.high_school": [True, True, True],
        "job_required_education.associates_degree": [True, False, False],
        "job_required_education.bachelors_degree": [True, True, False],
        "job_highlights.Qualifications": [["Relevant Bachelor Degree", "Analytics", "Team Environment Orientation"], 
                                            ["Some degree", "Something qualification1"],
                                            ["Some degree", "Something qualification1", "Something qualification2", "Something qualification3"]]
    })

    df_regions_input = pd.DataFrame({
        "state_code": ["NJ", "AR", "CA"],
        "state_name": ["New Jersey", "Arkansas", "California"],
        "region": ["Northeast", "South", "West"]
    })

    df_expected = pd.DataFrame({
        "job_id": ["j0tMZs6Zk4MAAAAAAAAAAA==", "HoRqzMmvw8EAAAAAAAAAAA==" ,"c60z-dt44zEAAAAAAAAAAA=="],
        "request_id": ["9ad8865c-462e-4f0a-9401-118f843993da", "9ad8865c-462e-4f0a-9401-118f843993da", "9ad8865c-462e-4f0a-9401-118f843993da"],
        "employer_name": ["SDG Group USA", "Staffigo Technical Services, LLC", "Bloomberg"],
        "employer_website": ["http://www.sdggroup.com", "http://www.staffigo.com", "http://www.bloomberg.com"],
        "job_employment_type": ["FULLTIME", "CONTRACTOR", "TEMPORARY"],
        "job_title": ["Data Analyst", "Data Engineer", "Data Scientist"],
        "job_description": ["Come join us! We need a Python expert", "Join our team for a SQL job", "Need an expert in cloud computing"],
        "job_is_remote": [True, False, True],
        "job_year": [2023, 2022, 2021],
        "job_month": [2, 6, 11],
        "job_day": [10, 2, 15],
        "job_city": ["Bedminster", "Bentonville", "Delano"],
        "job_state": ["NJ", "AR", "CA"],
        "job_region": ["Northeast", "South", "West"],
        "job_country": ["US", "US", "US"],
        "job_benefits_number": [4, 3, 2],
        "job_required_years_xp": [1.0, 4.0, 5.0],
        "job_highest_req_edu": ["Postgraduate degree", "Bachelors degree", "High School degree"],
        "job_min_salary": [50000, 100000, 150000],
        "job_max_salary": [55000, 105000, 155000],
        "job_qualifications_number": [3, 2, 4],
        "job_req_python": [True, False, False],
        "job_req_sql": [False, True, False],
        "job_req_cloud": [False, False, True]
    })

    # act 
    df_output = Transform.transform(df=df_input, df_regions=df_regions_input, request_id=request_id_input)

    # assert 
    pd.testing.assert_frame_equal(left=df_output, right=df_expected, check_exact=True)
