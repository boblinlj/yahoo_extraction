from modules.extract_yahoo_stats import YahooStats
from modules.extract_etf_stats import YahooETF
from modules.extract_yahoo_financials import YahooFinancial
from modules.extract_yahoo_consensus import YahooAnalysis
from modules.extract_nasdaq_universe import NasdaqUniverse
from util.transfer_data import UploadData2GCP
from util.helper_functions import create_log
from util.send_email import SendEmail
from util.database_management import DatabaseManagement
import pandas as pd
import datetime
import time
import sys
from configs import job_configs as jcfg


def check_if_job_exist(job_name) -> bool:
    """
    check if the job name exist in the job config file
    :param job_name: the job name
    :return: Boolean
    """
    return job_name in jcfg.JOB_DICT.keys()


def check_if_date_exist(date: str) -> bool:
    """
    check if the date string is a legit date
    :param date: date string in yyyy-mm-dd
    :return:Boolean
    """
    date_format = "%Y-%m-%d"
    try:
        res = bool(datetime.datetime.strptime(date, date_format))
    except ValueError:
        res = False
    return res


def check_if_frequency(frequency: str) -> bool:
    """
    check if the frequency exist in teh job config file
    :param frequency:frequency string
    :return:Boolean
    """
    return frequency in jcfg.FREQUENCY_LIST


def send_success_email_notifications(spider_object) -> None:
    """
    Send job successful email
    :param spider_object: job object
    :return: None
    """
    object_name = spider_object.__class__.__name__
    run_date = spider_object.updated_dt

    html = """\
    <html>
      <body>
        <p><b>{title}</b><br>
           {content}<br>
           <b>Data</b><br>
           {data}<br>
        </p>
      </body>
    </html>
    """

    table_list = jcfg.JOB_DICT[object_name]
    output_df = []

    for table_name in table_list:
        output_df.append(DatabaseManagement(table=table_name).table_update_summary())

    df_to_print = pd.concat(output_df)

    SendEmail(subject=f'[Do not reply]{object_name} Job has Finished - {run_date}',
              content=html.format(title=f'{object_name} job finished for {run_date}',
                                  content=f"""{'-' * 80} <br>"""
                                          f"""{spider_object.get_failed_extracts}/{spider_object.no_of_stock} Failed Extractions. <br>"""
                                          f"""The job made {spider_object.no_of_web_calls} calls through the internet. <br>"""
                                          f"""Log: {spider_object.loggerFileName}<br>"""
                                          f"""{'-' * 80}<br>""",
                                  data=df_to_print.to_html())).send()


def send_failure_email_notifications(job_name, run_date, error_message) -> None:
    """
    Send failure emails
    :param job_name: the job name
    :param run_date: the extraction date
    :param error_message: error message

    :return: None
    """
    SendEmail(subject=f'[Do not reply]{job_name} Job has Failed - {run_date}',
              content=error_message).send()


def run_main(job_name, run_date, frequency) -> None:
    """
    Main entrance for the data extraction jobs
    :param job_name: the name of jobs, the job must exist in the job config
    :param run_date: the extraction date
    :param frequency: the frequency of the job, this is used mainly in logs

    :return: None
    """

    # create log file
    loggerFileName = f"{frequency}_{job_name}_{datetime.date.today().strftime('%Y%m%d')}_{int(time.time())}.log"
    logger = create_log(loggerName=f'{frequency}_{job_name}', loggerFileName=loggerFileName)
    logger.info(f"{'*' * 80}\n")
    logger.info(f'{frequency} {job_name} started for {run_date}\n')
    logger.info(f"{'-' * 80}\n")
    logger.info(f'This job will population tables: {",".join(jcfg.JOB_DICT[job_name])}')
    logger.info(f"{'*' * 80}\n")

    # start the logic
    if not check_if_job_exist(job_name):
        logger.debug(f"Error: unknown job name {job_name}")
        return None
    elif not check_if_date_exist(run_date):
        logger.debug(f"Error: incorrect date {run_date}")
        return None
    else:
        # logic to run jobs
        if job_name == 'YahooStats':
            obj = YahooStats(updated_dt=run_date,
                             targeted_pop='PREVIOUS_POP',
                             batch=True,
                             loggerFileName=loggerFileName,
                             use_tqdm=False,
                             test_size=None)
        elif job_name == 'YahooETF':
            obj = YahooETF(updated_dt=run_date,
                           targeted_pop='YAHOO_ETF_ALL',
                           batch=True,
                           loggerFileName=loggerFileName,
                           use_tqdm=False,
                           test_size=None)
        elif job_name == 'YahooFinancial':
            obj = YahooFinancial(updated_dt=run_date,
                                 targeted_pop='PREVIOUS_POP',
                                 batch=True,
                                 loggerFileName=loggerFileName,
                                 use_tqdm=False,
                                 test_size=None)
        elif job_name == 'YahooAnalysis':
            obj = YahooAnalysis(updated_dt=run_date,
                                targeted_pop='PREVIOUS_POP',
                                batch_run=True,
                                loggerFileName=loggerFileName,
                                use_tqdm=False,
                                test_size=None)
        elif job_name == 'NasdaqUniverse':
            obj = NasdaqUniverse(updated_dt=run_date,
                                 loggerFileName=loggerFileName)
        else:
            obj = None

        # run object
        if obj is None:
            return None
        else:
            try:
                obj.run()
            except Exception as e:
                logger.debug(f"{job_name} failed {'*'*80}")
                logger.debug(e)
                send_failure_email_notifications(job_name=job_name, run_date=run_date, error_message=e)

        logger.info(f"{'*' * 80}\n")
        logger.info("Upload data to the cloud \n")
        logger.info(f"{'*' * 80}\n")
        # upload date to GCP
        tables = jcfg.JOB_DICT[job_name]
        UploadData2GCP(tables, loggerFileName=loggerFileName).run()

        send_success_email_notifications(obj)


if __name__ == "__main__":
    if len(sys.argv) == 4:
        run_date = datetime.datetime.today().date() - datetime.timedelta(days=int(sys.argv[2]))
        run_main(sys.argv[1], str(run_date), sys.argv[3])
    else:
        sys.stderr.write(f'Error: too many parameters, {sys.argv}')
