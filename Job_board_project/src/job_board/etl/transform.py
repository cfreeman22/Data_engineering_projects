import pandas as pd 
import datetime as dt
import math
import re

class Transform():

    @staticmethod
    def transform(
            df:pd.DataFrame,
            df_regions:pd.DataFrame,
            request_id: str
        )->pd.DataFrame:
        '''
        Performs transformation on dataframe produced from extract() function.
        - Takes in df
        - Takes in df_regions
        - Takes in request_id
        
        Returns:
        - a transformed dataframe
        '''
        # Add request_id
        df["request_id"] = request_id

        # Join region
        df = pd.merge(left=df, right=df_regions, left_on="job_state", right_on="state_code")

        # Column renaming
        df = df.rename(columns={
            "region": "job_region"
        })

        # Sort by most recent posting first
        df = df.sort_values(by="job_posted_at_datetime_utc", ascending=False).reset_index()

        # Filter to only full-time positions
        df = df[df["job_employment_type"]=="FULLTIME"]

        # Parse out YEAR, MONTH, DAY
        df["job_year"] = df["job_posted_at_datetime_utc"].apply(lambda x: dt.datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%fZ").year)
        df["job_month"] = df["job_posted_at_datetime_utc"].apply(lambda x: dt.datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%fZ").month)
        df["job_day"] = df["job_posted_at_datetime_utc"].apply(lambda x: dt.datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%fZ").day)

        # Create new columns
            # Number of benefits
        df["job_benefits_number"] = df["job_benefits"].apply(lambda x: Transform.apply_len(x))
            # Number of benefits
        df["job_required_years_xp"] = df["job_required_experience.required_experience_in_months"]/12
            # Number of required qualifications
        df["job_qualifications_number"] = df["job_highlights.Qualifications"].apply(lambda x: Transform.apply_len(x))
            # Highest education
        df['job_highest_req_edu'] = df.apply(lambda row: Transform.get_highest_education(row), axis=1)
            # Highest education
        df["job_req_python"] = df["job_description"].apply(lambda x: Transform.check_python(x))
        df["job_req_sql"] = df["job_description"].apply(lambda x: Transform.check_sql(x))
        df["job_req_cloud"] = df["job_description"].apply(lambda x: Transform.check_cloud(x))

        # Define columns that will be kep, and respective order
        keep_columns = ["job_id", "request_id", "employer_name", "employer_website", 
                        "job_employment_type", "job_title", "job_description", "job_is_remote","job_year",
                        "job_month","job_day","job_city","job_state","job_region","job_country",
                        "job_benefits_number","job_required_years_xp","job_highest_req_edu","job_min_salary","job_max_salary",
                        "job_qualifications_number","job_req_python","job_req_sql","job_req_cloud"]

        # Keep and order columns of interest only that will be kept in final dataframe
        df = df[keep_columns]

        # Quick print-out for visual confirmation that output is as expected during process
        print(df.columns)
        print(df.head())
        print(df.dtypes)

        return df

    @staticmethod
    def apply_len(list_input):
        '''
        Takes in a list, including where it is 'None' and returns its length or zero if empty or None
        '''
        if list_input is None or isinstance(list_input, float):
            length = 0
        else:
            length = len(list_input)
        return length

    @staticmethod
    def get_highest_education(row):
        '''
        Takes in a row of a dataframe and return the maximum education required (string) based on columns conditions
        '''
        if row['job_required_education.postgraduate_degree'] == True:
            return 'Postgraduate degree'
        elif row['job_required_education.bachelors_degree'] == True:
            return 'Bachelors degree'
        elif row['job_required_education.associates_degree'] == True:
            return 'Associates degree'
        elif row['job_required_education.high_school'] == True:
            return 'High School degree'
        else:
            return 'Non mentioned'

    @staticmethod
    def check_python(string):
        '''
        Takes in string and return True if finds regex mention of python, else returns False
        '''
        result = False
        if re.search("[Pp]ython", string):
            result = True
        return result

    @staticmethod
    def check_sql(string):
        '''
        Takes in string and return True if finds regex mention of sql, else returns False
        '''
        result = False
        if re.search("[Ss][Qq][Ll]", string):
            result = True
        return result

    @staticmethod
    def check_cloud(string):
        '''
        Takes in string and return True if finds regex mention of cloud or cloud platforms, else returns False
        '''
        result = False
        if re.search("AWS|GCP|Snowflake|Azure|[Cc]loud", string):
            result = True
        return result