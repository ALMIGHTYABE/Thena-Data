# Thena Data Documentation

## Overview
Thena Data is a Python-based data collection and reporting system that pulls data from various sources and writes it to Google Sheets. The system is designed to track and analyze data related to cryptocurrency pairs, fees, bribes, and revenue.


## Features
- Subgraph data collection for daily and pair statistics
- Contract data reading for pair name identification
- Automated Google Sheets integration
- Error handling and retry mechanisms
- Multiple data type support (bribes, fees, revenue, etc.)


## System Requirements

### Dependencies
```requirements.txt
requests==2.32.2
pandas==1.5.3
numpy==1.23.1
gspread==5.7.2
gspread-dataframe==3.3.0
PyYAML==6.0
web3==5.31.3
jmespath==1.0.1
```


## Configuration

The system uses a YAML configuration file (`params.yaml`) that contains:
- API endpoints
- Google Sheet keys
- Contract ABIs
- File paths
- Delta configurations