# Sales Data Analysis and Optimization Project

## Project Overview
This project demonstrates a comprehensive data engineering and analysis pipeline for a large-scale sales dataset. It showcases the implementation of a data warehouse, ETL processes, and the creation of data marts for targeted business insights.

## Key Features
- Processing of 1 million+ sales records
- Data storage and retrieval optimization using AWS S3
- Data transformation and analysis using Apache Spark
- Implementation of a star schema data model
- Creation of specialized data marts for customer and sales analysis
- Performance optimization resulting in 27% improvement in query speed

## Technical Stack
- Data Storage: AWS S3
- Data Processing: Apache Spark
- Data Warehousing: SQL Database 
- Data Modeling: Star Schema
- Programming Language: Python 

## Project Structure
1. **Data Generation**: 
   - Created a synthetic dataset with 1 million+ records
   - Columns: customer_id, store_id, sales_date, sales_person_id, price, quantity, total_cost

2. **Data Storage**:
   - Uploaded raw data to AWS S3
   - Implemented partitioning for optimized data retrieval

3. **Data Modeling**:
   - Designed a star schema with fact and dimension tables
   - Fact Table: Sales data
   - Dimension Tables: Customer, Store, Sales Team

4. **Data Transformation**:
   - Used Apache Spark to create dataframes
   - Performed joins between fact and dimension tables

5. **Data Mart Creation**:
   - Customer Data Mart: Aggregated by total purchase cost
   - Sales Data Mart: Aggregated by total sales

6. **Data Analysis and Optimization**:
   - Loaded data marts into SQL database
   - Created indexes for performance optimization
   - Achieved 27% improvement in overall query performance

## Business Use Cases
1. Identify top customers on a monthly basis for targeted coupon distribution
2. Recognize top-performing sales team members for monthly incentives

## Future Enhancements
- Implement real-time data ingestion
- Integrate machine learning models for predictive analytics
