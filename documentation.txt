### Part 1 Data Engineering
#### Dimensional model
**star schema** was used to create ta dimensional model
**dimension** tables include following: professionals (core professional attributes), skills, certificates, education, time, and industry

**fact** table includes following: jobs, and career progression

#### Data Validation and QC
- Validation of overlapping jobs
- Identification of expired certifications
- Outliers in years of experience
- Validation of date formats and NULLs

#### Assumption made
- current jobs have no end dates
- higher is better pay for salary band
- current role is the most recent job
- skill's year of experience is smaller or equal to total year of career

### Part 2 Requirements Gathering
1. question to ask the team to clarify
	1.  How to determine a successful career growth? what are the weights for each factor like salary growth, promotion speed, etc
	2. Any interested industries?
	3. Are we looking at young professionals with up to 3 of yoe or seniors or managers?
2. Key metrics
	1. promotion speed: avg time spent in each role before promotion
	2. skills: correlation between skills/certificates and career advancement
	3. Industry difference: salary band in each industry, does changing an industry brings huge salary increase?
3. How the data model would help
	1. the career progression fact table tracks metrics like salary growth, role changes and industry changes
	2. the skills and certs dimension table are linked with professionals and how they are correlated with each other
	3. time dimension allows to track career milesotnes and patterns of cohort
	4. job duration calculates the optimal timing for role changes
4. additional data sources
	1. market salary data
	2. industry level growth rate
	3. company info including company size, revenues, number of employees

### Part 3 Airflow
1. proper project structure was created
2. docker-composer.yaml was created as a sample
3. DAG code pipeline.py was created with few tasks missing including 
    - reading from MongoDB and convert to json
    - Taskgroup
    - validat_data task 
    - generate metrics task
4. dependencies are as following:
start >> extract_task >> transform_group >> validate_data_task >> generate_metrics_task >> load_task >> end