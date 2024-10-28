import csv
import requests
import json
import pandas as pd
from datetime import datetime
from collections import OrderedDict
import inquirer



API_ENDPOINT_CLUSTERS = 'https://api.cast.ai/v1/kubernetes/external-clusters' # List all clusters
API_ENDPOINT_SR = "https://api.cast.ai/v1/cost-reports/clusters/{}/savings?startTime={}&endTime={}" # Savings Report
API_ENDPOINT_CR = "https://api.cast.ai/v1/cost-reports/clusters/{}/cost?startTime={}&endTime={}" # Cost Report
API_ENDPOINT_UR = "https://api.cast.ai/v1/cost-reports/clusters/{}/resource-usage?startTime={}&endTime={}" # Usage Report
API_ENDPOINT_CW = "https://api.cast.ai/v1/kubernetes/clusters/{}/agent-status" # CASTware inventory

def select_customer():
    global API_TOKEN
    global ORG_ID
    global ORG
    with open('tokens.csv', mode='r') as file:
        csv_reader = csv.reader(file)
        data = list(csv_reader)

    menu_options = [row[0] for row in data[1:]]

    print("Please select an option:")
    for i, option in enumerate(menu_options, start=1):
        print(f"{i}. {option}")

    choice = int(input("Enter your choice (number): "))

    API_TOKEN = data[choice][1]
    ORG_ID = data[choice][2]
    ORG = data[choice][0]

def time_frame():

    global start_time
    global end_time

    years = [str(year) for year in range(2013, 2025)]
    months = [str(month) for month in range(1, 13)]
    days = [str(day) for day in range(1, 32)]
    print("Select Start Date:\n")
    year_prompt = [
        inquirer.List('year', message="Select year", choices=years)
    ]
    month_prompt = [
        inquirer.List('month', message="Select month", choices=months)
    ]
    day_prompt = [
        inquirer.List('day', message="Select day", choices=days)
    ]
    year = inquirer.prompt(year_prompt)['year']
    month = inquirer.prompt(month_prompt)['month']
    day = inquirer.prompt(day_prompt)['day']
    date_string = datetime(int(year), int(month), int(day)).strftime('%Y-%m-%d')
    year, month, day = map(int, date_string.split('-'))
    start_time = datetime(year, month, day).strftime("%Y-%m-%dT00:00:00.000000000Z")

    print("Select End Date:\n")
    year_prompt = [
        inquirer.List('year', message="Select year", choices=years)
    ]
    month_prompt = [
        inquirer.List('month', message="Select month", choices=months)
    ]
    day_prompt = [
        inquirer.List('day', message="Select day", choices=days)
    ]
    year = inquirer.prompt(year_prompt)['year']
    month = inquirer.prompt(month_prompt)['month']
    day = inquirer.prompt(day_prompt)['day']
    date_string = datetime(int(year), int(month), int(day)).strftime('%Y-%m-%d')
    year, month, day = map(int, date_string.split('-'))
    end_time = datetime(year, month, day).strftime("%Y-%m-%dT23:59:59.999999999Z")

def get_clusters(start_time, end_time):
    global headers
    headers = {
        'accept': 'application/json',
        'X-API-Key': API_TOKEN
    }
    response = requests.get(API_ENDPOINT_CLUSTERS, headers=headers)
    current_date = datetime.now().strftime('%Y%m%d%H%M%M%S')
    global clusterList_filename
    clusterList_filename = f"{current_date}.{ORG}_clusterList.csv"
    with open(f'./tmp/{clusterList_filename}', 'w') as clusterList_file:
        pass
    if response.status_code == 200:
        clusters = response.json()
        with open(f'./tmp/{clusterList_filename}', 'a') as clusterList_file:
            clusterList_file.write(f"clusterId,clusterName,organizationId\n")
	    
        forClusters(current_date,clusters, headers, clusterList_file, clusterList_filename)
    else:
        print(f"Failed to retrieve clusters. Status code: {response.status_code}, Response: {response.text}")
        exit()

def forClusters(current_date,clusters, headers, clusterList_file, clusterList_filename):
    for cluster in clusters['items']:
        cluster_id = cluster['id']
        cluster_name = cluster['name']
        with open(f'./tmp/{clusterList_filename}', 'a') as clusterList_file:
            clusterList_file.write(f"{cluster_id},{cluster_name},{ORG_ID}\n")
        cost_report(current_date,cluster_id,start_time,end_time,headers)
        usage_report(current_date,cluster_id,start_time,end_time,headers)
        castware_report(current_date,cluster_id,headers)
        dataAnalysis()

def cost_report(current_date,cluster_id, start_time, end_time, headers):
    global costReport_filename
    costReport_filename = f"{current_date}.{ORG}_costReport.json"
    with open(f'./tmp/{costReport_filename}', 'a') as clusterList_file:
        pass
    response = requests.get(API_ENDPOINT_CR.format(cluster_id, start_time, end_time), headers=headers)
    if response.status_code == 200:
        cost_report = response.json()
        with open(f'./tmp/{costReport_filename}', 'a') as costReport_file:
            costReport_file.write(response.text)
    elif response.status_code == 404:
        with open(f'./tmp/{costReport_filename}', 'a') as costReport_file:
            costReport_file.write(f"Cost report not found for Cluster ID {cluster_id} for the specified time range. It may not exist in the selected dates.\n")
    else:
        with open(f'./tmp/{costReport_filename}', 'a') as costReport_file:
            costReport_file.write(f"Failed to retrieve cost report for Cluster ID {cluster_id}. Status code: {response.status_code}, Response: {response.text}\n")

def usage_report(current_date,cluster_id, start_time, end_time, headers):
    global usageReport_filename
    response = requests.get(API_ENDPOINT_UR.format(cluster_id, start_time, end_time), headers=headers)
    usageReport_filename = f"{current_date}.{ORG}_usageReport.json"
    with open(f"./tmp/{usageReport_filename}",'a') as usageReport_file:
        pass
    if response.status_code == 200:
        usage_report = response.json()
        with open(f"./tmp/{usageReport_filename}", 'a') as usageReport_file:
            usageReport_file.write(response.text)
        #print(f"Usage Report for Cluster ID {cluster_id} from {start_time} to {end_time}: {json.dumps(usage_report, indent=2)}")
    elif response.status_code == 404:
        with open(f"./tmp/{usageReport_filename}", 'a') as usageReport_file:
            usageReport_file.write(f"Usage report not found for Cluster ID {cluster_id} for the specified time range. It may not exist in the selected dates.\n")
    else:
        with open(f"./tmp/{usageReport_filename}", 'a') as usageReport_file:
            usageReport_file.write(f"Failed to retrieve usage report for Cluster ID {cluster_id}. Status code: {response.status_code}, Response: {response.text}\n")

def castware_report(current_date, cluster_id, headers):
    global cwReport_filename
    response = requests.get(API_ENDPOINT_CW.format(cluster_id), headers=headers)
    cwReport_filename = f"{current_date}.{ORG}_cwReport.json"
    with open(f"./tmp/{cwReport_filename}",'a') as cwReport_file:
        pass
    if response.status_code == 200:
        response_data = response.json()
        ordered_data = OrderedDict()
        ordered_data["clusterId"] = cluster_id
        for key, value in response_data.items():
            ordered_data[key] = value

        with open(f"./tmp/{cwReport_filename}", 'a') as cwReport_file:
            cwReport_file.write(json.dumps(ordered_data, indent=4))
        #print(f"Usage Report for Cluster ID {cluster_id} from {start_time} to {end_time}: {json.dumps(usage_report, indent=2)}")
    elif response.status_code == 404:
        with open(f"./tmp/{cwReport_filename}", 'a') as cwReport_file:
            cwReport_file.write(f"Usage report not found for Cluster ID {cluster_id} for the specified time range. It may not exist in the selected dates.\n")
    else:
        with open(f"./tmp/{cwReport_filename}", 'a') as cwReport_file:
            cwReport_file.write(f"Failed to retrieve usage report for Cluster ID {cluster_id}. Status code: {response.status_code}, Response: {response.text}\n")

def dataAnalysis():
    try:
        cluster_df = pd.read_csv(f'./tmp/{clusterList_filename}')
        # Ensure expected columns are present
        if 'clusterId' not in cluster_df.columns or 'clusterName' not in cluster_df.columns:
            raise ValueError("Missing required columns in cluster list CSV")
    except Exception as e:
        raise ValueError(f"Error loading cluster CSV: {e}")

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

    cw_report = load_json(f'./tmp/{cwReport_filename}')
    usage_report = load_json(f'./tmp/{usageReport_filename}')
    cost_report = load_json(f'./tmp/{costReport_filename}')

    onboarded_phase = {}
    for report in cw_report:
        cluster_id = report['clusterId']
        agents = [status['name'] for status in report['statuses']]
        if set(agents) == {'castai-agent', 'castai-agent-cpvpa'}:
            onboarded_phase[cluster_id] = 'Read-Only'
        elif set(agents) == {'castai-agent-cpvpa','castai-evictor','castai-kvisor-controller','castai-agent','castai-spot-handler','castai-cluster-controller','castai-workload-autoscaler'}:
            onboarded_phase[cluster_id] = 'WOOP Connected'
        else:
            onboarded_phase[cluster_id] = 'Connected'

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

    usage_df = pd.DataFrame(usage_data)
    cost_df = pd.DataFrame(cost_data)

    merged_df = usage_df.merge(cost_df, on=['clusterId', 'month'], how='outer')
    merged_df['onboardedPhase'] = merged_df['clusterId'].map(onboarded_phase)

    cluster_name_map = dict(zip(cluster_df['clusterId'], cluster_df['clusterName']))
    merged_df['clusterName'] = merged_df['clusterId'].map(cluster_name_map)

    result_cluster_sum_df = merged_df.groupby(['clusterId', 'clusterName', 'onboardedPhase', 'month']).sum(numeric_only=True).reset_index()

    output_filepath = './analysis/Cluster_Monthly_Analysis_with_Breakdown.csv'
    result_cluster_sum_df.to_csv(output_filepath, index=False)

    import ace_tools_open as tools
    tools.display_dataframe_to_user(name="Cluster Monthly Analysis with Monthly Breakdown", dataframe=result_cluster_sum_df)

if __name__ == "__main__":
    select_customer()
    time_frame()
    get_clusters(start_time, end_time)