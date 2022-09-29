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




