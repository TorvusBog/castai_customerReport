import pandas as pd
import json
from datetime import datetime

# Load cluster list with error handling
try:
    cluster_df = pd.read_csv('../tmp/2024102620373744.DASA_clusterList.csv')
    # Ensure expected columns are present
    if 'clusterId' not in cluster_df.columns or 'clusterName' not in cluster_df.columns:
        raise ValueError("Missing required columns in cluster list CSV")
except Exception as e:
    raise ValueError(f"Error loading cluster CSV: {e}")

# Load JSON files with error handling
def load_json(filepath):
    data = []
    with open(filepath, 'r') as file:
        content = file.read()
        json_objects = content.split('}{')
        for i, obj in enumerate(json_objects):
            if i > 0:
                obj = '{' + obj
            if i < len(json_objects) - 1:
                obj = obj + '}'
            try:
                data.append(json.loads(obj))
            except json.JSONDecodeError:
                continue
    return data

cw_report = load_json('../tmp/2024102620373744.DASA_cwReport.json')
usage_report = load_json('../tmp/2024102620373744.DASA_usageReport.json')
cost_report = load_json('../tmp/2024102620373744.DASA_costReport.json')

# Process onboarded phase based on cwReport
onboarded_phase = {}
for report in cw_report:
    cluster_id = report['clusterId']
    agents = [status['name'] for status in report['statuses']]
    if set(agents) == {'castai-agent', 'castai-agent-cpvpa'}:
        onboarded_phase[cluster_id] = 'Read-Only'
    else:
        onboarded_phase[cluster_id] = 'Connected'

# Process usage and cost information
usage_data = []
cost_data = []

for report in usage_report:
    cluster_id = report['clusterId']
    for item in report['items']:
        timestamp = datetime.strptime(item['timestamp'], "%Y-%m-%dT%H:%M:%SZ")
        month = timestamp.strftime('%Y-%m')
        usage_data.append({
            'clusterId': cluster_id,
            'month': month,
            'cpuRequested': float(item['cpuRequested']),
            'cpuProvisioned': float(item['cpuProvisioned'])
        })

for report in cost_report:
    cluster_id = report['clusterId']
    for item in report['items']:
        timestamp = datetime.strptime(item['timestamp'], "%Y-%m-%dT%H:%M:%SZ")
        month = timestamp.strftime('%Y-%m')
        total_cost = float(item['costOnDemand']) + float(item['costSpot'])
        cost_data.append({
            'clusterId': cluster_id,
            'month': month,
            'totalCost': total_cost
        })

# Convert lists to DataFrames
usage_df = pd.DataFrame(usage_data)
cost_df = pd.DataFrame(cost_data)

# Merge usage and cost data
merged_df = usage_df.merge(cost_df, on=['clusterId', 'month'], how='outer')
merged_df['onboardedPhase'] = merged_df['clusterId'].map(onboarded_phase)

# Add cluster name
cluster_name_map = dict(zip(cluster_df['clusterId'], cluster_df['clusterName']))
merged_df['clusterName'] = merged_df['clusterId'].map(cluster_name_map)

# Group by cluster and month
result_df = merged_df.groupby(['clusterId', 'clusterName', 'month', 'onboardedPhase']).sum(numeric_only=True).reset_index()

# Display final DataFrame
import ace_tools_open as tools
tools.display_dataframe_to_user(name="Cluster Monthly Analysis", dataframe=result_df)

