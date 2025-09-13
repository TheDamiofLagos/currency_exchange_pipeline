# Exchange Rate Data Pipeline

## Overview

This data pipeline extracts exchange rate data from the Open Exchange API and processes it through a series of transformations for analytical use.

## Data Pipeline Architecture

### Data Flow
1. **Fivetran** pulls exchange rate data from Open Exchange API
2. Data is loaded into **Google Cloud Storage** as Parquet files
3. **Google Cloud Data Transfers** moves the Parquet files to BigQuery tables
4. **dbt** performs transformations on the BigQuery data

## Data Sources and Storage

### BigQuery Tables
- **Dataset**: `open-exchange-us.rates_raw`
- **Tables**:
  - `open_exchange_currency_raw`
  - `open_exchange_rates_raw`

## dbt Transformation Layers

### 1. Base Models
- **Purpose**: 1:1 representation with source tables
- **Description**: Direct mapping of raw BigQuery tables

### 2. Prep Models  
- **Purpose**: Light transformation layer
- **Description**: Basic data cleaning and preparation

### 3. Dimensional Models
- **Purpose**: Final analytical models
- **Description**: Business-ready tables for reporting and analysis

## Technology Stack

- **Data Extraction**: Fivetran
- **Data Storage**: Google Cloud Storage (Parquet files)
- **Data Warehouse**: BigQuery
- **Data Transfer**: Google Cloud Data Transfers
- **Data Transformation**: dbt