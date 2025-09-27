# Data Platform Product Requirements Document (PRD)

## 1. Data Platform Architecture Overview

### Current Architecture

![](course_assets/project_walkthrough_images/00_02_data_platform_architecture.png)

[Describe the existing data platform in 1-2 paragraphs. Include a simple diagram showing data flow from sources to analytics.]

**Key Components:**
- Data Sources: [List main data sources]
- Ingestion: [How data gets into the system]
- Database: [PostgreSQL setup]
- Data Lake: [MinIO storage]
- ETL Pipelines: [Data processing]

## 2. Gap Analysis

### 2.1 Ingestion into Transactional Database

Here's the current state in markdown format:

**Current State:**

- **Schema validation**: Uses JSON schema files to define table structure with specific data types

- **Data cleaning**: Automatically removes empty rows and standardizes column names

- **Duplicate prevention**: Implements UPSERT operations using `ON CONFLICT` to handle duplicate primary keys

- **Audit trails**: Adds automatic `pg_created_at` and `pg_updated_at` timestamps with update triggers

- **Idempotent processing**: Can run the same ingestion multiple times without creating duplicates

- **Type safety**: Enforces PostgreSQL data types defined in schema (BIGINT, TEXT, DATE, INTEGER, etc.)

- **Primary key constraints**: Ensures data uniqueness through primary key definitions

- **Comprehensive logging**: Detailed logging at DEBUG, INFO, and ERROR levels for troubleshooting

- **Transaction safety**: Uses atomic database transactions to ensure all data operations complete entirely or not at all, preventing partial data corruption

- **Error handling**: Validates file existence and includes proper exception handling

**Gaps Found:**
- [Gap 1]
- [Gap 2]
**Recommendations:**
- [Recommendation 1]
- [Recommendation 2]

### 2.2 Transactions on Transactional Database
**Current State:** [Brief description]
**Gaps Found:**
- [Gap 1]
- [Gap 2]
**Recommendations:**
- [Recommendation 1]
- [Recommendation 2]

### 2.3 Replication to Data Lakehouse
**Current State:** [Brief description]
**Gaps Found:**
- [Gap 1]
- [Gap 2]
**Recommendations:**
- [Recommendation 1]
- [Recommendation 2]

## 3. Improvement Suggestions

### Priority 1 (High Impact)
**Improvement:** [Title]
- What: [Brief description]
- Why: [Benefits]
- Tradeoffs: [Pros and cons]
- Test Results: [What you found when testing]

### Priority 2 (Medium Impact)
**Improvement:** [Title]
- What: [Brief description]
- Why: [Benefits]
- Tradeoffs: [Pros and cons]
- Test Results: [What you found when testing]

### Priority 3 (Low Impact)
**Improvement:** [Title]
- What: [Brief description]
- Why: [Benefits]
- Tradeoffs: [Pros and cons]
- Test Results: [What you found when testing]

## 4. Remaining Questions

**Questions outside this project scope:**
- [Question 1]: [Why it's important but not covered]
- [Question 2]: [Why it's important but not covered]
- [Question 3]: [Why it's important but not covered]

## 5. Next Steps

**Immediate Actions:**
- [Action 1]
- [Action 2]

**Future Considerations:**
- [Consideration 1]
- [Consideration 2]
