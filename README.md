# Yelp Big Data ETL & Analytics Platform

A fully automated, production-grade data pipeline that transforms raw Yelp JSON into clean, analytics-ready Parquet data using AWS, Databricks (PySpark), and Neo4j.

**Tech Stack**: PySpark · AWS S3 · Lambda · Glue · Athena · QuickSight · Neo4j · Databricks  
**Key Features**: Modular ETL · Event-Driven Ingestion · Sentiment Analysis · Graph Modeling · Visual Dashboards  
![image](https://github.com/user-attachments/assets/63aa793d-4ad8-4687-b871-f73bff2b06ce)

## Table of Contents
1. [Introduction and Background](#1-introduction-and-background)  
2. [AWS and Databricks Setup](#2-aws-and-databricks-setup)  
3. [Databricks ETL and Workflow Architecture](#3-databricks-etl-and-workflow-architecture)  
   - [Business Rules for ETL Processing](#business-rules-for-etl-processing)  
4. [Lambda Function as Orchestration Trigger](#4-lambda-function-as-orchestration-trigger)  
5. [Glue Crawler and Athena Table Creation](#5-glue-crawler-and-athena-table-creation)  
6. [Visualization and Machine Learning](#6-visualization-and-machine-learning)  
7. [Graph Analytics with Neo4j AuraDB](#7-graph-analytics-with-neo4j-auradb)  
8. [Conclusion and Practical Significance](#8-conclusion-and-practical-significance)

## 1. Introduction and Background

### 1.1 Introduction

In today’s data-driven business world, companies heavily rely on feedback, reviews, and interactions from users to drive product and service improvement. The Yelp Open Dataset provides publicly available data related to local businesses, including business metadata, user reviews, and social connections.

Our goal is to build a **scalable, automated ETL pipeline** using **AWS and Databricks** to transform this semi-structured data into analytics-ready tables, supporting querying, visualization, and ML use cases.

### 1.2 Background

The Yelp dataset includes:
- **Business**: metadata about businesses (location, hours, etc.)
- **Review**: reviews with text, ratings, and feedback
- **User**: user profiles and social info

Challenges:
- Deeply nested JSON
- Data cleaning and normalization
- Feature engineering (sentiment, engagement)

### 1.3 Project Overview

This project demonstrates:
- Integration of S3, Lambda, Glue, Athena, QuickSight, Databricks
- Automated ETL across Business, Review, and User domains
- Unified analytics-ready table
- Support for both BI (QuickSight) and ML (Databricks)
- Graph analytics via Neo4j

## 2. AWS and Databricks Setup

- **S3**: Raw and processed data lake
- **IAM Roles**: Secure access to S3
- **Databricks**: Subscribed via AWS Marketplace
- **S3 Mounting**: Using `dbutils.fs.mount()`

IAM permissions:  
- `s3:GetObject`, `PutObject`, `DeleteObject`

Buckets:
- `yelprawdata`
- `yelpprocesseddata`

## 3. Databricks ETL and Workflow Architecture

ETL layers:
- **Bronze**: Raw JSON → Delta
- **Silver**: Flattened & cleaned
- **Gold**: Business logic

Modular notebooks:
- `Business_ETL.ipynb`
- `Review_ETL.ipynb`
- `User_ETL.ipynb`
- `UnifiedAnalytics.ipynb`

Orchestrated using **Databricks Jobs** with DAG dependencies.

### Business Rules for ETL Processing

#### Business
- Drop null `business_id`, missing `categories`
- Clean `hours`, flatten schema

#### Review
- Create `engagement_score`
- Apply VADER sentiment analysis

#### User
- `elite_years_count`, `engagement_compliments`
- Drop raw `useful`, `funny`, `cool`

#### UnifiedAnalytics
- Join on `user_id`, `business_id`
- Rename columns to avoid conflicts
- Left joins to retain max info

![Image](https://github.com/user-attachments/assets/ad1493bf-0fca-4d29-877a-d4dc9861d2b0)

## 4. Lambda Function as Orchestration Trigger

- Triggered on new file in `yelprawdata` S3 bucket
- Calls Databricks API to trigger ETL
- Smart logic: triggers UnifiedAnalytics only after all three domains are present
- Glue Crawler also triggered from Lambda

## 5. Glue Crawler and Athena Table Creation

- **Glue Crawler** scans Parquet and updates schema
- **Athena** used for querying unified table
  
![Image](https://github.com/user-attachments/assets/89a0589f-7eae-4758-b7b4-bf7539279299)


## 6. Visualization and Machine Learning

### QuickSight (BI):
- Connect to Athena
- Visualize sentiment by city, elite user distribution, category trends

### Databricks (ML):
- Build ML models:
  - Predict review rating
  - Classify elite users
  - Cluster businesses

---
![Image](https://github.com/user-attachments/assets/534f6c7a-8881-4f85-99ac-309bebdb930b)

## 7. Graph Analytics with Neo4j AuraDB



**Why Neo4j?**
- Model social networks and business-review-user relationships
- Run graph algorithms: PageRank, community detection, etc.

**Nodes**:
- `User`, `Business`, `Review`, `Category`, `City`, `State`

**Relationships**:
- `WROTE`, `REVIEWS`, `FRIENDS_WITH`, `HAS_CATEGORY`, `LOCATED_IN`

Data is exported as micro-batch CSVs to GCS, then imported via Cypher using `LOAD CSV`.

---

## 8. Conclusion and Practical Significance

This project demonstrates:
- Real-world ETL using cloud-native tools
- Handling nested JSON and transforming it into relational and graph formats
- Modular, event-driven orchestration using Lambda and Databricks Jobs
- Cross-platform data exploration (QuickSight, Neo4j, Athena)

🚀 **Key Takeaways**:
- Use Glue + Athena + QuickSight for serverless BI
- Use Databricks for Spark-based processing and ML
- Use Neo4j for relationship and community graph analysis

