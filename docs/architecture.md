# Architecture Overview

## System Architecture

This platform is a cloud-native ESG analytics SaaS built on Google Cloud.

### Components

1. Frontend
   - Web dashboard
   - Hosted on Cloud Run

2. Backend API
   - FastAPI application
   - Deployed on Cloud Run

3. Analytics Layer
   - Google BigQuery
   - Stores processed emissions data

4. Storage
   - Google Cloud Storage
   - Stores uploaded CSV files

5. Authentication
   - Google OAuth2

6. Monitoring
   - Cloud Monitoring
   - Error Reporting

### Data Flow

User → Frontend → Backend API → BigQuery → Dashboard

CSV Upload → Cloud Storage → Processing → BigQuery

### Security Boundary

Each customer has isolated datasets and access policies.

## High Availability

- Multi-zone Cloud Run
- Managed BigQuery backend
- Automatic scaling
