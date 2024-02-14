# Crypto Data Streaming and Analysis with Apache Spark and Hive

This repository contains a project that leverages Apache Spark, Hive, and Spark Streaming to stream and analyze cryptocurrency data from [Tiingo](https://www.tiingo.com/) via WebSocket. The project facilitates real-time data processing and storage into Hadoop Distributed File System (HDFS).

## Overview
The workflow involves:

1. Establishing a WebSocket connection to Tiingo's crypto website to fetch real-time data.
2. Streaming the data through Apache Spark Streaming for real-time processing.
3. Storing the streaming data in HDFS for further analysis and long-term storage.
4. Executing Spark jobs to process and filter the data.

## Features

- **WebSocket Connection**: Establishes a connection to Tiingo's WebSocket API to fetch real-time cryptocurrency data.
- **Spark Streaming**: Utilizes Apache Spark Streaming for real-time processing of the data stream.
- **HDFS Storage**: Stores the streaming data in HDFS, enabling scalable and fault-tolerant storage.
- **Spark Jobs**: Executes Spark jobs to process, filter, and transform the data.
- **Hive Integration**: Integrates with Hive for structured data storage, enabling easy querying and visualization.

## Usage

To use this project, follow these steps:

1. Clone the repository to your local machine.
2. Set up Apache Spark and Hive on your environment if not already installed.
3. Configure the WebSocket connection to Tiingo's crypto website in the provided configuration files.
4. Run the Spark streaming application to start fetching and processing real-time data.
5. Execute Spark jobs to process and filter the data as per your requirements.
6. Save the processed data into Hive tables.

## Dependencies

- Apache Spark
- Apache Hive
- Tiingo API
- Hadoop HDFS
- Java Web Sockets API

---

**Disclaimer:** This project is for educational and informational purposes only. It does not provide financial advice. Always do your own research before making any investment decisions.
