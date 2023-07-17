from database.postgres import PostgresDB
from job_board.etl.extract import Extract
from job_board.etl.transform import Transform
from job_board.etl.load import Load
#from utility.metadata_logging import something
import os 
import logging
import yaml 
from io import StringIO
from utility.metadata_logging import MetadataLogging
import datetime as dt
# from apscheduler.schedulers.background import BackgroundScheduler   
import time


def pipeline_per_job_title(config, run_log)->bool:
    '''
    Pipeline function to run extraction, transformation, and loading (ETL) per job title. Takes in:
    - Config file with job title specfication and other target parameters
    - run_log parameter (StringIO)
    '''
    
    # start the streamIO from a clean slate 
    run_log.seek(0)
    run_log.truncate(0)

    api_key_id = os.environ.get("api_key_id")

    metadata_logger = MetadataLogging()
        
    metadata_logger_table = f"metadata_log_{config['load']['database']['target_table_name']}"
    metadata_logger_run_id = metadata_logger.get_latest_run_id(db_table=metadata_logger_table)
    metadata_logger.log(
        run_timestamp=dt.datetime.now(),
        run_status="started",
        run_id=metadata_logger_run_id, 
        run_config=config,
        db_table=metadata_logger_table
    )

    logging.info(f"Commencing pipeline for {config['extract']['title']}⏳")
    logging.info("Commencing extraction")
    
    # extract data 
    try:
        
        request_id, df = Extract.extract(
            job_title=config["extract"]["title"], 
            api_key_id=api_key_id
        )

        df_region_codes = Extract.extract_region_codes("job_board/data/usa_regions.csv")

        logging.info(f"Extraction complete for job title : {config['extract']['title']}")

        logging.info("Commencing transformation")
        # transform data
        df_transform = Transform.transform(
            df=df,
            df_regions=df_region_codes,
            request_id=request_id
        )
        logging.info("Transformation complete")

        # load file (upsert)
        logging.info("Commencing file load")
        Load.load(
            df=df_transform,
            load_target=config["load"]["file"]["load_target"],
            target_file_directory=config["load"]["file"]["target_file_directory"],
            target_file_name=config["load"]["file"]["target_file_name"],
        )   
        logging.info("File load complete")

        engine = PostgresDB.create_pg_engine()

        # load database (upsert)
        logging.info("Commencing database load")
        Load.load(
            df=df_transform,
            load_target=config["load"]["database"]["load_target"],
            target_database_engine=engine,
            target_table_name=config["load"]["database"]["target_table_name"]
        )  
        logging.info("Database load complete")
        logging.info(f"Pipeline for {config['extract']['title']} complete ✅")

        metadata_logger.log(
            run_timestamp=dt.datetime.now(),
            run_status="completed",
            run_id=metadata_logger_run_id, 
            run_config=config,
            run_log=run_log.getvalue(),
            db_table=metadata_logger_table
        )
        
        
        return True
    except BaseException as e: 
        logging.exception(e)
        metadata_logger.log(
            run_timestamp=dt.datetime.now(),
            run_status="error",
            run_id=metadata_logger_run_id, 
            run_config=config,
            run_log=run_log.getvalue(),
            db_table=metadata_logger_table
        )
    print(run_log.getvalue())

def pipeline()->bool:
    '''
    Iteratively runs the pipeline_per_job_title job per job title in the config file
    '''
    print("job running")
    run_log = StringIO()
    logging.basicConfig(level=logging.INFO, stream=run_log,format="[%(levelname)s][%(asctime)s][%(filename)s]: %(message)s") # format: https://docs.python.org/3/library/logging.html#logging.LogRecord
    
    # get yaml config 
    with open("job_board/config.yaml") as stream:
        config = yaml.safe_load(stream)

    for job_title in config["job_titles"]:
        pipeline_per_job_title(config=job_title['job_title'], run_log=run_log)
    
    return True 


if __name__ == "__main__": 
    
    # run the pipeline
    if pipeline():
        print("success")