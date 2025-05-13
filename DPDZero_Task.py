import os
import logging
import pandas as pd
import argparse
import DPDClass

# Setup logging
if not os.path.exists("logs"):
    os.makedirs("logs")
logging.basicConfig(
    filename='logs/pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Create a DPDClass object
logging.info("Creating DPDClass object")
dpd = DPDClass.DPDClass()

def dpdtask(call_log_path, agent_roster_path, disposition_summary_path):

    # 1.Ingest and validate data
    #For the file call_logs.csv
    call_logs = dpd.ingest_validate_data(call_log_path, primary_key_cols=['agent_id', 'org_id', 'call_date'])

    #For the file agent_roster.csv
    agent_roster = dpd.ingest_validate_data(agent_roster_path, primary_key_cols=['agent_id', 'org_id'])

    #For the file disposition_summary.csv
    disposition_summary = dpd.ingest_validate_data(disposition_summary_path, primary_key_cols=['agent_id', 'org_id', 'call_date'])


    # 2. Merging/joining the dataframes
    logging.info("Merging dataframes call_logs, agent_roster")
    merged_data = dpd.merge_dataframes(call_logs, agent_roster, on_cols=['agent_id', 'org_id'], mtype='left')

    logging.info("Merging dataframes previously merged with disposition_summary")
    merged_data = dpd.merge_dataframes(merged_data, disposition_summary, on_cols=['agent_id', 'org_id', 'call_date'], mtype='left')

    # 3. Feature engineering
    agent_performance_summary = dpd.feature_engg(merged_data)

    # 4. Output
    if not os.path.exists("result"):
        os.makedirs("result")
    agent_performance_summary.to_csv('result/agent_performance_summary.csv', index=False)
    logging.info(f"Saved csv report to 'result/agent_performance_summary.csv'")

    #Getting the slack summary message
    slack_summary = dpd.slack_summary_message(agent_performance_summary, agent_performance_summary['call_date'].max())
    logging.info("Slack summary message generated successfully")

    return slack_summary

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='DPDZero Task data pipeline')
    parser.add_argument('--call_logs', type=str, required=True, help='Path to call_logs.csv')
    parser.add_argument('--agent_roster', type=str, required=True, help='Path to agent_roster.csv')
    parser.add_argument('--disposition_summary', type=str, required=True, help='Path to disposition_summary.csv file')

    args = parser.parse_args()

    # Call the dpdtask function with the provided arguments
    print(dpdtask(args.call_logs, args.agent_roster, args.disposition_summary))
    