[Data-Set-1 AGentLoggingReport] (https://drive.google.com/file/d/1WrG-9qv6atP-W3P_-gYln1hHyFKRKMHP/view
)

[Data-set-2 Agent Performance] (https://drive.google.com/file/d/1-JIPCZ34dyN6k9CqJa-Y8yxIGq6vTVXU/view)

Note: both files are csv files.)]

## 1. Create a schema based on the given dataset

          CREATE TABLE agent_performance
    >   (
    >      id int,
    >      date string,
    >      total_chats int,
    >      average_response_time string,
    >      average_resolution_time string,
    >      rating float,
    >      feedback int
    >      )

    row format DELIMITED
    fields TERMINATED by ',';

## 2. Dump the data inside the hdfs in the given schema location.

## 3. List of all agents' names.

        SELECT agent_name FROM agent_performance_part_bucket

## 4. Find out agent average rating.

    hive> SELECT avg(rating) as avg_rating FROM agent_performance_part_bucket;

    avg_rating
    1.4609629649255012
    Time taken: 46.589 seconds, Fetched: 1 row(s)
    hive>

## 5. Total working days for each agents

#### 5.1

    - 30 days for each agent

#### 5.2 **Query**

    with unique_table as (SELECT distinct agent_name, date FROM agent_performance_part_bucket)

    SELECT agent_name, count(date) as working_day FROM unique_table GROUP BY agent_name;




