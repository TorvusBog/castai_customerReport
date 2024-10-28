# CAST AI Cluster Reporting Script Documentation

## Overview

This Python script allows users to fetch and export data for clusters managed in CAST AI. The script retrieves reports on cluster costs, usage, and agent statuses, and provides a summary analysis of monthly data. The data fetched is saved in JSON and CSV formats for further examination and usage.

## Requirements

The script relies on specific Python libraries for data handling, date manipulation, and API requests. Ensure that the following libraries are installed by using the provided `requirements.txt` file.

### requirements.txt

```bash
pip install -r requirements.txt
```

### Tokens

A file called tokens.csv.example, change it to tokens.csv and add the tokens in the following order (follow the header row):

```csv
organizatioName,apiToken,OrganizationID
```

## Running

Make sure that tmp and analysis directory exists

and run it with

```bash
python3 getCustomerData.py
```

It will create temp files in the working directory 'tmp' and the analysis output in the 'analysis' directory



