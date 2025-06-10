## Flink Sales Streaming Data Pipeline 

#### Overview 
![flowchart_flink_sales_fix](https://github.com/user-attachments/assets/8edfb6e9-de14-469c-81e2-45a366475095)

This project is a Streaming data pipeline project that the output is a dashbord. This project using kafka for real-time data streaming and Apache Flink for ETL Streaming for execute query table, and then clickhouse for database, and grafane for creating dashbord.

#### Features 
- Data Generation: Python script generates table sales data and save it as csv
- Stream Data Generation: Python script that streaming or produce data into kafka
- Flink Python Job: Python Job Flink for execute table-table query
- Connector Kafka Connect: Connector clickhouse kafka connect that insert data from kafka into clickhouse
- Dashbord Grafana: Using Plugin clickhouse for datasource and make dashbord from data in clickhouse

### Technologies Used 
- Python
- Apache Kafka
- Apache Flink
- Connector Clickhouse Kafka Connect
- Clickhouse
- Grafana
- Docker


