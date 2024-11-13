import pandas as pd
import re
import json
import argparse
from datetime import timedelta

def load_data(file_path):
    # Load the data and convert the date column to datetime
    df = pd.read_csv(file_path)
    df['date'] = pd.to_datetime(df['date'], errors='coerce', utc=True)
    return df

def extract_components(df):
    # Extract components/modules within square brackets using regex
    df['component'] = df['message'].str.extract(r'\[([^\]]+)\]')
    components = df['component'].dropna().unique()
    return components

def calculate_dora_metrics(df, module_name):
    # Filter commits related to the specified module
    module_commits = df[df['message'].str.contains(rf'\[{re.escape(module_name)}\]', case=False, na=False)].copy()

    # Identify deployment and failure-related commits
    deployment_keywords = ['deploy', 'release', 'bump', 'update', 'version']
    failure_keywords = ['bug','rollback', 'hotfix', 'revert', 'patch']
    bug_keywords = ['bug', 'error', 'issue']

    module_commits['is_deployment'] = module_commits['message'].str.contains('|'.join(deployment_keywords), case=False, na=False)
    module_commits['is_failure'] = module_commits['message'].str.contains('|'.join(failure_keywords), case=False, na=False)
    module_commits['is_bug'] = module_commits['message'].str.contains('|'.join(bug_keywords), case=False, na=False)
    module_commits['is_pr'] = module_commits['message'].str.contains('PR|pull request', case=False, na=False)

    # Remove timezone information before converting to periods to avoid warnings
    module_commits['date'] = module_commits['date'].dt.tz_localize(None)

    # Calculate Deployment Frequency (Weekly)
    deployment_count = int(module_commits['is_deployment'].sum())
    if deployment_count > 0:
        module_commits['week'] = module_commits['date'].dt.to_period('W')
        weekly_deployment_freq = module_commits[module_commits['is_deployment']].groupby('week').size().mean()
    else:
        weekly_deployment_freq = 0

    # Calculate Change Failure Rate
    failure_count = int(module_commits['is_failure'].sum())
    change_failure_rate = (failure_count / deployment_count * 100) if deployment_count > 0 else 0

    # Calculate Mean Time to Recovery (MTTR)
    recovery_times = []
    failure_indices = module_commits[module_commits['is_failure']].index

    for idx in failure_indices:
        failure_time = module_commits.loc[idx, 'date']
        subsequent_recoveries = module_commits[(module_commits.index > idx) & (module_commits['is_deployment'])]
        if not subsequent_recoveries.empty:
            recovery_time = subsequent_recoveries.iloc[0]['date']
            time_to_recovery = (recovery_time - failure_time).total_seconds() / 3600  # Time in hours
            if time_to_recovery > 0:
                recovery_times.append(time_to_recovery)

    mean_time_to_recovery = sum(recovery_times) / len(recovery_times) if recovery_times else 0

    # Calculate Lead Time for Changes (assuming commit-to-deployment as lead time)
    lead_times = []
    if deployment_count > 0:
        deployment_times = module_commits[module_commits['is_deployment']]['date']
        lead_times = (deployment_times - module_commits['date']).dt.total_seconds() / 3600  # Time in hours
        lead_time_for_changes = lead_times.mean() if not lead_times.empty else 0
    else:
        lead_time_for_changes = 0

    # Calculate total number of commits, bugs, and PRs
    total_commits = int(len(module_commits))
    bug_count = int(module_commits['is_bug'].sum())
    pr_count = int(module_commits['is_pr'].sum())

    return {
        "Module Name": module_name,
        "Total Commits": total_commits,
        "Total Bugs": bug_count,
        "Total PRs": pr_count,
        "Weekly Deployment Frequency": float(weekly_deployment_freq),
        "Change Failure Rate (%)": float(change_failure_rate),
        "Mean Time to Recovery (Hours)": float(mean_time_to_recovery),
        "Lead Time for Changes (Hours)": float(lead_time_for_changes)
    }

def main():
    parser = argparse.ArgumentParser(description='Analyze DORA metrics from a CSV file.')
    parser.add_argument('file_path', type=str, help='Path to the CSV file to analyze')
    args = parser.parse_args()

    # Load data
    df = load_data(args.file_path)

    # Extract components/modules based on messages in square brackets
    components = extract_components(df)

    # Calculate and store DORA metrics for each component
    results = []
    for component in components:
        dora_metrics = calculate_dora_metrics(df, component)
        results.append(dora_metrics)

    # Output results as JSON
    json_output = json.dumps(results, indent=4)
    print(json_output)

    # Save results to a JSON file
    with open('dora_metrics_output.json', 'w') as json_file:
        json_file.write(json_output)

if __name__ == "__main__":
    main()
