# IMDB Hadoop MapReduce Project

This repository contains a Java-based Hadoop MapReduce project that processes IMDB datasets using a multi-stage MapReduce pipeline. The program was designed to run both locally and on a SLURM-managed Hadoop cluster and focuses on correctness, scalability, and performance analysis.

---

## Overview

The application runs two sequential MapReduce jobs:

**Job 1**

* Reads IMDB title and actor datasets
* Filters for movies only
* Performs a reduce-side join between titles and actors
* Emits intermediate results keyed by actor and year

**Job 2**

* Reads the output of Job 1
* Re-maps the data to enable aggregation
* Counts the number of movies per year for a specific actor

This separation ensures that keys and values are structured correctly across MapReduce stages.

---

## Key Design Decisions

* Multiple MapReduce jobs are used to avoid key and value loss between stages
* Mapper output types are explicitly defined and differ from reducer output types where needed
* A custom mapper is used in Job 2 to reformat input keys for aggregation
* Reducer counts are configurable to support experimentation
* Input split size is configurable to analyze mapper parallelism and performance
* `@Override` annotations are used for clarity and compile-time safety

---

## Performance Observations

* Smaller input split sizes create more mapper tasks and increase overhead
* Larger split sizes (64–128 MB) reduce mapper count and improve runtime
* Part B generally takes longer due to additional data shuffling and aggregation

Execution time can be measured using SLURM accounting tools (`sacct`) or Hadoop job logs.

---

## Technologies Used

* Java
* Hadoop 3.2.2
* HDFS
* SLURM
* Bash
