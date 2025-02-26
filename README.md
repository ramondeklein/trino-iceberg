# Iceberg with Trino

## Initialize
Bring up the environment by running the following command:
```sh
docker-compose up
```

This will spin up the following containers:
1. [MinIO](https://min.io) holding the object store (persisted to the `data` volume).
1. [Nessie](https://projectnessie.org/) holding the catalog (as an alternative for Hive).
1. [Trino](https://trino.io/) implementing the Iceberg-compatible query engine.

It will also spin up an one-time container `mc` that creates the required buckets in the object-store.

The Trino server takes some time (up to 30 seconds) to initialize. Once it has initialized it will log `======== SERVER STARTED =======` and it is ready to accept commands.

## Create the initial table
Start the Trino shell using the following command:
```sh
docker-compose exec -it trino trino
```

Then create the schema called `test` in the `cat` catalog:
```sql
CREATE SCHEMA cat.test WITH (location = 's3://warehouse/cat');
```
And the table:
```sql
CREATE TABLE cat.test.app_logging
(
  timestamp   TIMESTAMP(3),
  application VARCHAR(30),
  machine     VARCHAR(30),
  message     VARCHAR(1000)
)
WITH (
  format = 'AVRO',
  partitioning = ARRAY['application','machine','hour(timestamp)'],
  sorted_by = ARRAY['timestamp']
);
```

This will create a table using the [AVRO format](https://en.wikipedia.org/wiki/Apache_Avro) called `app_logging` that is partitioned on the following three columns:
- `application`
- `machine`
- `timestamp` (rounded to the hour).

### Inspect the actual files
Make sure the [MinIO client (`mc`)](https://min.io/docs/minio/linux/reference/minio-mc.html) is installed locally and run the following commands:
```sh
mc alias set trino http://localhost:9000 admin password
mc ls -r trino/warehouse/cat/
```
This will show all the files for this table and it will be limited to two metadata files (actual names may differ):
```
[2025-02-26 19:44:20 CET] 2.8KiB STANDARD app_logging-551097253b2948caa2aa155dcdd04580/metadata/00000-4ab95d05-58bb-43df-88bd-4e3fbca2f05c.metadata.json
[2025-02-26 19:44:20 CET] 4.2KiB STANDARD app_logging-551097253b2948caa2aa155dcdd04580/metadata/snap-3368065610902075646-1-4acb3400-5f88-40f3-8b8c-5d5965cf215c.avro
```
The metadata can be inspected using the following command (adjust the filename to what you got in the previous command):
```sh
mc cat trino/warehouse/cat/app_logging-551097253b2948caa2aa155dcdd04580/metadata/00000-4ab95d05-58bb-43df-88bd-4e3fbca2f05c.metadata.json | jq .
```
This will show something like this:
```json
{
  "format-version": 2,
  "table-uuid": "8f8fef87-5715-4019-af46-50b51b2d671e",
  "location": "s3://warehouse/cat/app_logging-551097253b2948caa2aa155dcdd04580",
  "last-sequence-number": 1,
  "last-updated-ms": 1740595460781,
  "last-column-id": 4,
  "current-schema-id": 0,
  "schemas": [
    {
      "type": "struct",
      "schema-id": 0,
      "fields": [
        {
          "id": 1,
          "name": "timestamp",
          "required": false,
          "type": "timestamp"
        },
        {
          "id": 2,
          "name": "application",
          "required": false,
          "type": "string"
        },
        {
          "id": 3,
          "name": "machine",
          "required": false,
          "type": "string"
        },
        {
          "id": 4,
          "name": "message",
          "required": false,
          "type": "string"
        }
      ]
    }
  ],
  "default-spec-id": 0,
  "partition-specs": [
    {
      "spec-id": 0,
      "fields": [
        {
          "name": "application",
          "transform": "identity",
          "source-id": 2,
          "field-id": 1000
        },
        {
          "name": "machine",
          "transform": "identity",
          "source-id": 3,
          "field-id": 1001
        },
        {
          "name": "timestamp_hour",
          "transform": "hour",
          "source-id": 1,
          "field-id": 1002
        }
      ]
    }
  ],
  "last-partition-id": 1002,
  "default-sort-order-id": 1,
  "sort-orders": [
    {
      "order-id": 1,
      "fields": [
        {
          "transform": "identity",
          "source-id": 1,
          "direction": "asc",
          "null-order": "nulls-first"
        }
      ]
    }
  ],
  "properties": {
    "write.format.default": "AVRO",
    "write.parquet.compression-codec": "zstd",
    "commit.retry.num-retries": "4"
  },
  "current-snapshot-id": 3368065610902075646,
  "refs": {
    "main": {
      "snapshot-id": 3368065610902075646,
      "type": "branch"
    }
  },
  "snapshots": [
    {
      "sequence-number": 1,
      "snapshot-id": 3368065610902075646,
      "timestamp-ms": 1740595460781,
      "summary": {
        "operation": "append",
        "trino_query_id": "20250226_184424_00002_bcy5z",
        "trino_user": "trino",
        "changed-partition-count": "0",
        "total-records": "0",
        "total-files-size": "0",
        "total-data-files": "0",
        "total-delete-files": "0",
        "total-position-deletes": "0",
        "total-equality-deletes": "0",
        "engine-version": "471",
        "engine-name": "trino",
        "iceberg-version": "Apache Iceberg 1.7.1 (commit 4a432839233f2343a9eae8255532f911f06358ef)"
      },
      "manifest-list": "s3://warehouse/cat/app_logging-551097253b2948caa2aa155dcdd04580/metadata/snap-3368065610902075646-1-4acb3400-5f88-40f3-8b8c-5d5965cf215c.avro",
      "schema-id": 0
    }
  ],
  "statistics": [],
  "partition-statistics": [],
  "snapshot-log": [
    {
      "timestamp-ms": 1740595460781,
      "snapshot-id": 3368065610902075646
    }
  ],
  "metadata-log": []
}
```
The metadata contains the schema of the table and also the partitioning information. It also holds the snapshot that is part of Iceberg's feature to perform point-in-time queries. The actual manifest list is stored in an AVRO table, that can be inspected by downloading the AVRO tools using the following command:
```sh
curl -O -J https://dlcdn.apache.org/avro/avro-1.11.3/java/avro-tools-1.11.3.jar 
```
Make sure Java is installed (`sudo apt install default-jre` on Ubuntu) and check if `java -jar avro-tools-1.11.3.jar` dumps all the tools.

Now you can dump the snapshot:
```sh
mc cat trino/warehouse/cat/app_logging-551097253b2948caa2aa155dcdd04580/metadata/snap-3368065610902075646-1-4acb3400-5f88-40f3-8b8c-5d5965cf215c.avro | java -jar avro-tools-1.11.3.jar tojson - | jq .
```
It won't show anything, because there is no data yet.

### Insert some data
Start the Trino shell using the following command:
```sh
docker-compose exec -it trino trino
```
Now insert some data:
```sql
INSERT INTO cat.test.app_logging (timestamp,application,machine,message) VALUES
(TIMESTAMP '2025-02-26 13:00:01', 'test 1', 'machine1', 'This is an example log message'),
(TIMESTAMP '2025-02-26 13:00:02', 'test 1', 'machine2', 'This is an example log message'),
(TIMESTAMP '2025-02-26 13:00:03', 'test 1', 'machine1', 'This is an example log message'),
(TIMESTAMP '2025-02-26 13:00:04', 'test 1', 'machine2', 'This is an example log message'),
(TIMESTAMP '2025-02-26 13:00:05', 'test 2', 'machine1', 'This is an example log message'),
(TIMESTAMP '2025-02-26 13:00:06', 'test 2', 'machine2', 'This is an example log message'),
(TIMESTAMP '2025-02-26 13:00:07', 'test 2', 'machine1', 'This is an example log message'),
(TIMESTAMP '2025-02-26 13:00:08', 'test 2', 'machine2', 'This is an example log message');
```

The data can be fetched using the following command:
```sql
SELECT * FROM cat.test.app_logging;
```

### Inspect the files
Now check the data files again:
```sh
mc ls -r trino/warehouse/cat/
```
It will now show both data and metadata:
```
[2025-02-26 20:03:06 CET]   860B STANDARD app_logging-551097253b2948caa2aa155dcdd04580/data/application=test+1/machine=machine1/timestamp_hour=2025-02-26-13/20250226_190502_00003_bcy5z-1375c20e-75f3-496d-af13-d58017d49191.avro
[2025-02-26 20:03:06 CET]   860B STANDARD app_logging-551097253b2948caa2aa155dcdd04580/data/application=test+1/machine=machine2/timestamp_hour=2025-02-26-13/20250226_190502_00003_bcy5z-1a0e3dbb-5c91-4866-902d-464fa6d3b42c.avro
[2025-02-26 20:03:06 CET]   860B STANDARD app_logging-551097253b2948caa2aa155dcdd04580/data/application=test+2/machine=machine1/timestamp_hour=2025-02-26-13/20250226_190502_00003_bcy5z-795818e7-744f-4f2d-9b6c-47dd83611979.avro
[2025-02-26 20:03:06 CET]   860B STANDARD app_logging-551097253b2948caa2aa155dcdd04580/data/application=test+2/machine=machine2/timestamp_hour=2025-02-26-13/20250226_190502_00003_bcy5z-81d12be2-ea32-4baf-8108-61a17868bd39.avro
[2025-02-26 19:44:20 CET] 2.8KiB STANDARD app_logging-551097253b2948caa2aa155dcdd04580/metadata/00000-4ab95d05-58bb-43df-88bd-4e3fbca2f05c.metadata.json
[2025-02-26 20:03:06 CET] 4.1KiB STANDARD app_logging-551097253b2948caa2aa155dcdd04580/metadata/00001-4cf52ce1-7224-41d7-b9d1-3d68bcc56069.metadata.json
[2025-02-26 20:03:07 CET] 5.4KiB STANDARD app_logging-551097253b2948caa2aa155dcdd04580/metadata/00002-555e9bc6-5959-4e79-aa48-56f57d55a313.metadata.json
[2025-02-26 20:03:07 CET] 1.0KiB STANDARD app_logging-551097253b2948caa2aa155dcdd04580/metadata/20250226_190502_00003_bcy5z-8dac1ae5-badc-4382-a3d2-33d1ecd150bd.stats
[2025-02-26 20:03:06 CET] 7.4KiB STANDARD app_logging-551097253b2948caa2aa155dcdd04580/metadata/99f0e08b-59bb-4af1-8fc0-18410378e12c-m0.avro
[2025-02-26 20:03:06 CET] 4.4KiB STANDARD app_logging-551097253b2948caa2aa155dcdd04580/metadata/snap-1625673523733626400-1-99f0e08b-59bb-4af1-8fc0-18410378e12c.avro
[2025-02-26 19:44:20 CET] 4.2KiB STANDARD app_logging-551097253b2948caa2aa155dcdd04580/metadata/snap-3368065610902075646-1-4acb3400-5f88-40f3-8b8c-5d5965cf215c.avro
```
The data structure shows the partitioning scheme nicely. It also shows updated metadata and an updated snapshot that now does contain some data:
```sh
mc cat trino/warehouse/cat/app_logging-551097253b2948caa2aa155dcdd04580/metadata/snap-1625673523733626400-1-99f0e08b-59bb-4af1-8fc0-18410378e12c.avro | java -jar avro-tools-1.11.3.jar tojson - | jq .
```
It now shows:
```json
{
  "manifest_path": "s3://warehouse/cat/app_logging-551097253b2948caa2aa155dcdd04580/metadata/99f0e08b-59bb-4af1-8fc0-18410378e12c-m0.avro",
  "manifest_length": 7620,
  "partition_spec_id": 0,
  "content": 0,
  "sequence_number": 2,
  "min_sequence_number": 2,
  "added_snapshot_id": 1625673523733626400,
  "added_files_count": 4,
  "existing_files_count": 0,
  "deleted_files_count": 0,
  "added_rows_count": 8,
  "existing_rows_count": 0,
  "deleted_rows_count": 0,
  "partitions": {
    "array": [
      {
        "contains_null": false,
        "contains_nan": {
          "boolean": false
        },
        "lower_bound": {
          "bytes": "test 1"
        },
        "upper_bound": {
          "bytes": "test 2"
        }
      },
      {
        "contains_null": false,
        "contains_nan": {
          "boolean": false
        },
        "lower_bound": {
          "bytes": "machine1"
        },
        "upper_bound": {
          "bytes": "machine2"
        }
      },
      {
        "contains_null": false,
        "contains_nan": {
          "boolean": false
        },
        "lower_bound": {
          "bytes": "¥`\u0007\u0000"
        },
        "upper_bound": {
          "bytes": "¥`\u0007\u0000"
        }
      }
    ]
  },
  "key_metadata": null
}
```
The manifest looks like this:
```sh
mc cat trino/warehouse/cat/app_logging-551097253b2948caa2aa155dcdd04580/metadata/99f0e08b-59bb-4af1-8fc0-18410378e12c-m0.avro | java -jar avro-tools-1.11.3.jar tojson - | jq .
```
```json
{
  "status": 1,
  "snapshot_id": {
    "long": 1625673523733626400
  },
  "sequence_number": null,
  "file_sequence_number": null,
  "data_file": {
    "content": 0,
    "file_path": "s3://warehouse/cat/app_logging-551097253b2948caa2aa155dcdd04580/data/application=test+2/machine=machine2/timestamp_hour=2025-02-26-13/20250226_190502_00003_bcy5z-81d12be2-ea32-4baf-8108-61a17868bd39.avro",
    "file_format": "AVRO",
    "partition": {
      "application": {
        "string": "test 2"
      },
      "machine": {
        "string": "machine2"
      },
      "timestamp_hour": {
        "int": 483493
      }
    },
    "record_count": 2,
    "file_size_in_bytes": 860,
    "column_sizes": null,
    "value_counts": null,
    "null_value_counts": null,
    "nan_value_counts": null,
    "lower_bounds": null,
    "upper_bounds": null,
    "key_metadata": null,
    "split_offsets": null,
    "equality_ids": null,
    "sort_order_id": {
      "int": 0
    }
  }
}
{
  "status": 1,
  "snapshot_id": {
    "long": 1625673523733626400
  },
  "sequence_number": null,
  "file_sequence_number": null,
  "data_file": {
    "content": 0,
    "file_path": "s3://warehouse/cat/app_logging-551097253b2948caa2aa155dcdd04580/data/application=test+1/machine=machine2/timestamp_hour=2025-02-26-13/20250226_190502_00003_bcy5z-1a0e3dbb-5c91-4866-902d-464fa6d3b42c.avro",
    "file_format": "AVRO",
    "partition": {
      "application": {
        "string": "test 1"
      },
      "machine": {
        "string": "machine2"
      },
      "timestamp_hour": {
        "int": 483493
      }
    },
    "record_count": 2,
    "file_size_in_bytes": 860,
    "column_sizes": null,
    "value_counts": null,
    "null_value_counts": null,
    "nan_value_counts": null,
    "lower_bounds": null,
    "upper_bounds": null,
    "key_metadata": null,
    "split_offsets": null,
    "equality_ids": null,
    "sort_order_id": {
      "int": 0
    }
  }
}
{
  "status": 1,
  "snapshot_id": {
    "long": 1625673523733626400
  },
  "sequence_number": null,
  "file_sequence_number": null,
  "data_file": {
    "content": 0,
    "file_path": "s3://warehouse/cat/app_logging-551097253b2948caa2aa155dcdd04580/data/application=test+1/machine=machine1/timestamp_hour=2025-02-26-13/20250226_190502_00003_bcy5z-1375c20e-75f3-496d-af13-d58017d49191.avro",
    "file_format": "AVRO",
    "partition": {
      "application": {
        "string": "test 1"
      },
      "machine": {
        "string": "machine1"
      },
      "timestamp_hour": {
        "int": 483493
      }
    },
    "record_count": 2,
    "file_size_in_bytes": 860,
    "column_sizes": null,
    "value_counts": null,
    "null_value_counts": null,
    "nan_value_counts": null,
    "lower_bounds": null,
    "upper_bounds": null,
    "key_metadata": null,
    "split_offsets": null,
    "equality_ids": null,
    "sort_order_id": {
      "int": 0
    }
  }
}
{
  "status": 1,
  "snapshot_id": {
    "long": 1625673523733626400
  },
  "sequence_number": null,
  "file_sequence_number": null,
  "data_file": {
    "content": 0,
    "file_path": "s3://warehouse/cat/app_logging-551097253b2948caa2aa155dcdd04580/data/application=test+2/machine=machine1/timestamp_hour=2025-02-26-13/20250226_190502_00003_bcy5z-795818e7-744f-4f2d-9b6c-47dd83611979.avro",
    "file_format": "AVRO",
    "partition": {
      "application": {
        "string": "test 2"
      },
      "machine": {
        "string": "machine1"
      },
      "timestamp_hour": {
        "int": 483493
      }
    },
    "record_count": 2,
    "file_size_in_bytes": 860,
    "column_sizes": null,
    "value_counts": null,
    "null_value_counts": null,
    "nan_value_counts": null,
    "lower_bounds": null,
    "upper_bounds": null,
    "key_metadata": null,
    "split_offsets": null,
    "equality_ids": null,
    "sort_order_id": {
      "int": 0
    }
  }
}
```

### Insert some data
Start the Trino shell again using the following command:
```sh
docker-compose exec -it trino trino
```
Now insert some more data:
```sql
INSERT INTO cat.test.app_logging (timestamp,application,machine,message) VALUES
(TIMESTAMP '2025-02-26 13:01:01', 'test 1', 'machine1', 'This is an example log message'),
(TIMESTAMP '2025-02-26 13:01:02', 'test 1', 'machine2', 'This is an example log message'),
(TIMESTAMP '2025-02-26 13:01:03', 'test 1', 'machine1', 'This is an example log message'),
(TIMESTAMP '2025-02-26 13:01:04', 'test 1', 'machine2', 'This is an example log message'),
(TIMESTAMP '2025-02-26 13:01:05', 'test 2', 'machine1', 'This is an example log message'),
(TIMESTAMP '2025-02-26 13:01:06', 'test 2', 'machine2', 'This is an example log message'),
(TIMESTAMP '2025-02-26 13:01:07', 'test 2', 'machine1', 'This is an example log message'),
(TIMESTAMP '2025-02-26 13:01:08', 'test 2', 'machine2', 'This is an example log message');
```
This will add 4 more data files (the data is distributed across 4 different partitions), so the total number of data files is now 8. Each `INSERT` statement will add a new datafile, snapshot, ... so this quickly adds up. Try running the following command:
```sql
SELECT COUNT(*) FROM cat.test.app_logging;
```
It will show 16 records and it uses 17 splits. Now combine the datafiles by issuing the following command and request the count again:
```sql
ALTER TABLE cat.test.app_logging EXECUTE optimize;
SELECT COUNT(*) FROM cat.test.app_logging;
```
It will now show 13 splits and it combined the partitions from the 1st and 2nd INSERT. This is also considered a write operation, so it also creates a new snapshot and metadata files.

Optimizing may be an expensive operation, but it can also be run so it only optimizes recent files. When the command is run regularly, then it's fine to only optimize the latest added files:
```sql
ALTER TABLE cat.test.app_logging EXECUTE optimize WHERE "$file_modified_time" > current_date - interval '1' day;
```

### Removing old snapshots and orphaned files
The actual number of files is increasing, but old snapshots can be deleted by issuing the following commands:
```sql
ALTER TABLE cat.test.app_logging EXECUTE expire_snapshots(retention_threshold => '0s');    -- Delete the snapshots
ALTER TABLE cat.test.app_logging EXECUTE remove_orphan_files(retention_threshold => '0s'); -- Remove orphaned files
```
Note that `cat.properties` contains the following two lines to enable deleting snapshots and orphaned files:
```ini
iceberg.expire-snapshots.min-retention=0s
iceberg.remove-orphan-files.min-retention=0s
```
Without these settings, only snapshots and orphaned files older than 7 days can be removed.

## Indexing
TODO...