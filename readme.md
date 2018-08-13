## Intro

This repository stands for Elasticsearch Management scripts written in Java as Juit tests. This repo provides below tests.

- Delete old Indices
- Change replica for specific indices
- Change refresh interval for specific indices

## Advantage

If you're running a big cluster using Elasticsearch, you need to manage count of indices and to keep the number as lower as possible. This repo helps you to consider this concerns.

## Usage

You can run a batch job using Jenkins with below commands.

    $ mvn -Des.host=hostname -Des.port=port -Dtest=EsIndexManagementTest#changeRefreshTimeInOldIndices clean test 