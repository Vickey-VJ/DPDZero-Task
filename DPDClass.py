import logging
from pathlib import Path
import pandas as pd

class DPDClass:

    # for Data Ingestion and Validation
    def ingest_validate_data(self, data_path: str, primary_key_cols):
        try:
            logging.info(f"Ingesting and validating data of {Path(data_path).stem}")
            logging.info(f"Reading CSV files in the data directory: {data_path}")
            df = pd.read_csv(data_path)
            logging.info("CSV files read successfully")

            logging.info("Validating data")
            if df.empty:
                logging.error("Dataframe are empty")
                raise ValueError("Files are empty")
                return

            df = df.drop_duplicates()

            for col in primary_key_cols:
                if col not in df.columns:
                    logging.error(f"Required key column {col} is missing in {data_path}")
                elif df[col].isnull().any():
                    null_count = df[col].isnull().sum()
                    df[col].remove_na(inplace=True)
                    logging.warning(f"Column {col} has {null_count} are removed {data_path}")
                else:
                    logging.info(f"Column {col} is valid")

            for col in primary_key_cols:
                if "date" in col.lower():
                    logging.info(f"Date column found: {col}, Validating date format")
                    df[col] = pd.to_datetime(df[col], format='%Y-%m-%d')
                    df[col] = df[col].dt.strftime('%Y-%m-%d')
                    logging.info(f"Formatted {col} column of {Path(data_path).stem} successfully")
            
            logging.info(f"Data of {Path(data_path).stem} is ingested and validated successfully")

            return df

        except Exception as e:
            logging.error(f"Data validation error: {e}")
            #exit(1)
            return pd.DataFrame()
    
    # for Merging/Joins data
    def merge_dataframes(self, df1, df2, on_cols, mtype):
        try:
            logging.info("Merging dataframes...")
            merged_df = pd.merge(df1, df2, on=on_cols, how=mtype)
            logging.info("Dataframes merged successfully")

            logging.info(f"Merged: {len(merged_df)} rows and {len(merged_df.columns)} columns")

            missing_data = merged_df[merged_df['users_first_name'].isnull()]
            if not missing_data.empty:
                logging.warning(f"Missing data found in merged dataframe: {missing_data['agent_id'].nunique()}")

            return merged_df
        
        except Exception as e:
            logging.error(f"Error merging dataframes: {e}")
            return pd.DataFrame()
        
    # for Feature Engineering
    def feature_engg(self, df):
        logging.info("Starting feature engineering phase - agent performance summary")
        df['completed'] = df['status'].str.lower().eq('completed')
        df['duration_min'] = df['duration'] / 60.0
        df['presence'] = df['login_time'].notnull().astype(int)

        performance_summary = df.groupby(['agent_id', 'users_first_name', 'users_last_name', 'call_date']).agg(
            total_calls=('call_id', 'count'),
            unique_loans=('installment_id', 'nunique'),
            completed_calls=('completed', 'sum'),
            avg_duration_min=('duration_min', 'mean'),
            presence=('presence', 'max')
        ).reset_index()

        performance_summary['connect_rate'] = (performance_summary['completed_calls'] / performance_summary['total_calls']).round(2)
        performance_summary['avg_duration_min'] = performance_summary['avg_duration_min'].round(2)

        logging.info(f"Computed metrics for {performance_summary.shape[0]} agent-data combinations")

        return performance_summary

    #For formatting the slack summary message
    def slack_summary_message(self, df, date):
        try:
            logging.info(f"Generating slack summary message for {date}")
            report = df[df['call_date'] == date]
            if report.empty:
                logging.warning(f"No data found for {date}")
                return f"No agent data found for {date}"

            top_agent = report.sort_values(by='connect_rate', ascending=False).iloc[0]

            return (f"Agent Summary for {date}\n"
                    f"Top Performer: {top_agent['users_first_name']} {top_agent['users_last_name']} ({int(top_agent['connect_rate'] * 100)}% connect rate)\n"
                    f"Total Active Agents: {report['presence'].sum()}\n"
                    f"Average Duration: {round(report['avg_duration_min'].mean(), 2)}")
        
        except Exception as e:
            logging.error(f"Error generating slack summary message: {e}")
            return "Error generating slack summary message"