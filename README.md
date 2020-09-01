# sfloader
Snowflake File Loader

Note: This software is a community project released under the Apache 2.0 License, not official Snowflake software.

sfloader puts files from an on premises source to a Snowflake managed stage. While putting files to a Snowflake managed stage is simple, putting hundreds of thousands or millions of files introduces some challenges such as bandwidth management, parallelism, and keeping track of successful uploads.

sfloader collects an inventory of local files and stores metadata on them in a lightweight (SQLite) database. It manages the putting of files to a Snowflake stage with parallel threads and tracking of each attempted put.
