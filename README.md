# Scala Spark Day 26 — Real-Time Healthcare Streaming

## 1. Project Overview

This project is part of the 30-Day Scala and Apache Spark practice series. It demonstrates real-time healthcare event processing using Apache Spark Streaming and DStreams.

The application receives patient vital-sign readings through a TCP socket, validates the input, identifies abnormal readings using broadcast thresholds, tracks cumulative abnormal readings per patient, and calculates abnormal-event counts over a moving window.

This is an educational simulation using synthetic patient data, not a clinically validated monitoring system.

## 2. Objectives

- Design a patient-vital event schema.
- Process incoming vital-sign streams.
- Generate alerts for abnormal readings.
- Use broadcast variables to distribute reference thresholds.
- Use an accumulator to demonstrate distributed counting.
- Track repeated abnormal readings with stateful processing.
- Calculate abnormal readings over a time window.
- Understand Spark partitions, DAG execution, and fault tolerance.

## 3. Technologies Used

| Technology | Version |
|---|---|
| Scala | 2.13.16 |
| Apache Spark | 3.5.6 |
| Spark Streaming | 3.5.6 |
| SBT | 1.10.11 |
| Java | 17 |
| Environment | Ubuntu on WSL2 |
| Input source | TCP socket |

## 4. Problem Statement

Healthcare monitoring systems can receive a continuous stream of vital-sign events from patient-monitoring devices. The system needs to process these events, compare values with reference thresholds, identify abnormal readings, and highlight patients with repeated abnormal readings.

This project simulates that workflow using Spark Streaming.

## 5. Patient Event Schema

Each incoming event follows this format:

`PATIENT_ID,VITAL_TYPE,VALUE`

Example:

`P001,HEART_RATE,120`

Fields:

- `PATIENT_ID`: identifier for the simulated patient.
- `VITAL_TYPE`: type of vital being measured.
- `VALUE`: numeric measurement.

Supported vital types and illustrative thresholds:

| Vital type | Minimum | Maximum |
|---|---:|---:|
| HEART_RATE | 60 | 100 |
| SPO2 | 95 | 100 |
| TEMPERATURE | 36.1 | 37.5 |

These values are demonstration thresholds only. Actual clinical interpretation depends on factors such as patient characteristics, measurement conditions, and validated clinical guidance.

## 6. Project Structure

```text
scala-day26/
├── src/
│   └── main/
│       └── scala/
│           └── Day26.scala
├── day26-result.txt
├── build.sbt
├── .gitignore
└── README.md
```

### File descriptions

- `Day26.scala`: Spark Streaming implementation.
- `build.sbt`: Scala version, Spark dependencies, and Java runtime options.
- `day26-result.txt`: complete actual terminal output from the streaming execution.
- `.gitignore`: excludes generated build and IDE files.
- `README.md`: project documentation.

## 7. Streaming Configuration

The application uses a `StreamingContext` with a five-second batch interval and reads data from a TCP socket.

- Batch interval: 5 seconds
- Socket host: `localhost`
- Socket port: `9996`
- Window duration: 15 seconds
- Window slide: 5 seconds
- Checkpoint directory: `/tmp/scala-day26-checkpoint`

The application uses `local[2]` for local execution with two processing threads.

## 8. Broadcast Variables

The vital thresholds are stored in a broadcast variable:

```scala
val thresholds = sc.broadcast(Map(
  "HEART_RATE" -> (60.0, 100.0),
  "SPO2" -> (95.0, 100.0),
  "TEMPERATURE" -> (36.1, 37.5)
))
```

Broadcast variables distribute read-only reference data to Spark tasks. This avoids repeatedly embedding the same small threshold map in each task's processing logic.

## 9. Abnormal Vital Detection

Incoming lines are parsed into patient ID, vital type, and numeric value. Malformed records are ignored.

The application compares each recognized vital type against its configured minimum and maximum. A value outside that range is classified as abnormal for this simulation.

Abnormal records produce an alert containing the patient ID, vital type, and value.

## 10. Accumulator

The application uses a `LongAccumulator` to demonstrate counting abnormal records processed by the streaming application.

The accumulator is updated when abnormal records are collected for output.

**Important limitation:** Spark accumulators are useful for metrics and diagnostics, but retries or recomputation can affect their totals. They should not be used as an exact, durable audit ledger.

## 11. Stateful Processing

The application uses `updateStateByKey` to maintain a cumulative count of abnormal readings for each patient.

Conceptually:

1. Extract the patient ID from every abnormal event.
2. Convert each event into a count of one.
3. Combine new counts with the previously stored count.
4. Display each patient's cumulative abnormal-reading count.
5. Print a repeated-alert message when the count reaches three or more.

This demonstrates how stateful processing retains information across micro-batches.

## 12. Window Processing

The application uses a 15-second window with a five-second sliding interval to calculate the number of abnormal readings in recent streaming data.

- Window duration controls how much recent data is considered.
- Slide duration controls how frequently the calculation updates.
- The window count represents recent activity, while the stateful patient count is cumulative.

## 13. Checkpointing

The application configures checkpointing:

```scala
ssc.checkpoint("/tmp/scala-day26-checkpoint")
```

Checkpointing stores information required for stateful streaming recovery and helps Spark restore streaming state after supported failures.

The checkpoint directory is local to the development environment and is not included in the Git repository.

## 14. Sample Input

Start the socket server and send synthetic events such as:

```text
P001,HEART_RATE,120
P002,SPO2,88
P001,TEMPERATURE,39.0
P003,HEART_RATE,75
P001,HEART_RATE,130
P001,SPO2,90
P002,HEART_RATE,115
P003,TEMPERATURE,37.0
```

Normal example readings should not generate alerts under the configured demonstration thresholds. Abnormal readings should generate alerts, and repeated readings should increase the cumulative count.

## 15. Execution Commands

### Compile

```bash
cd ~/scala-day26
sbt clean compile
```

### Start the TCP socket server

Open a second terminal:

```bash
nc -lk 9996
```

### Run the application

In the project terminal:

```bash
cd ~/scala-day26
sbt run 2>&1 | tee day26-result.txt
```

Enter the sample events in the socket terminal. Stop the streaming application with `Ctrl+C` when testing is complete.

## 16. Execution Results

The complete actual terminal output is stored in `day26-result.txt`.

The execution confirmed:

- Spark Streaming started successfully.
- The socket receiver connected to port `9996`.
- Broadcast thresholds were created.
- Checkpointing and window processing ran.
- Six abnormal readings were detected in a test batch.
- A repeated-abnormal-reading alert was generated for patient `P001`.

The result file contains the detailed Spark logs and processing output. It is intentionally not duplicated in this README.

## 17. Spark Partitions, DAG, and Fault Tolerance

### Partitions

Spark divides distributed data into partitions so that tasks can process different portions of data in parallel.

### DAG

A Directed Acyclic Graph (DAG) represents the sequence of transformations and dependencies Spark uses to plan processing stages.

### Fault tolerance

Spark can recompute lost partitions using lineage. Streaming checkpointing also preserves state and metadata needed for supported recovery scenarios. Recovery behavior depends on the input source, checkpoint configuration, and processing design.

## 18. Processing Flow

```text
TCP Socket
    |
    v
Receive Patient Vital Events
    |
    v
Parse and Validate Records
    |
    v
Compare Against Broadcast Thresholds
    |
    v
Filter Abnormal Readings
    |
    +---------------------------+
    |             |             |
    v             v             v
Alerts       Accumulator    Window Count
                  |
                  v
          Stateful Patient Counts
                  |
                  v
          Repeated-Reading Alerts
```

## 19. Key Learnings

- DStreams and micro-batch processing
- Patient-vital event schema design
- Broadcast variables
- Accumulators and their limitations
- Abnormal-event filtering
- Stateful processing with `updateStateByKey`
- Window operations
- Checkpointing
- Partitions and DAG execution
- Spark fault-tolerance concepts

## 20. Conclusion

Day 26 demonstrates a real-time healthcare event-processing workflow using Scala and Spark Streaming. It combines broadcast thresholds, abnormal-vital alerts, accumulator-based metrics, cumulative per-patient counts, window processing, and checkpointing.

The project provides a foundation for understanding streaming analytics in a healthcare-style scenario. It is intended for learning and does not replace validated clinical monitoring systems.

**Project status:** Day 26 implementation and local execution completed.

**Execution log:** `day26-result.txt`
