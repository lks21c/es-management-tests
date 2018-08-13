## Intro

This repository stands for Elasticsearch Management scripts written in Java as Juit tests. This repo provides below tests.

- Delete old Indices
- Change replica for specific indices
- Change refresh interval for specific indices

## Concept

This script premises that you create and manage index with "yyyymmdd" or "yyyymmddhh" pattern.

- Delete old Indices
    - Delete indices older than 7 days. 
- Change replica for specific indices
    - Change replica for realtime indices to increase indexing speed and to gain stability.
- Change refresh interval for specific indices
    - Change refresh interval for older than today.


## Advantage

If you're running a big cluster using Elasticsearch, you need to manage count of indices and to keep the number as lower as possible. This repo helps you to consider this concerns.

## Usage

You can run a batch job using Jenkins with below commands.

    $ mvn -Des.host=hostname -Des.port=port -Dtest=EsIndexManagementTest#changeRefreshTimeInOldIndices clean test 