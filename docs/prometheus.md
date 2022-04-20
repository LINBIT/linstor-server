# Prometheus Metrics

The LINSTOR controller exposes metrics that can be scraped by Prometheus. The
endpoint used for scraping metrics is exposed on `127.0.0.1:3370/metrics` on the
LINSTOR controller.

## LINSTOR Cluster Metrics

- `linstor_info`: Versioning information for the LINSTOR controller.
- `linstor_node_state`: Node type and state for each node in LINSTOR.
- `linstor_resource_definition_count`: Number of resources currently managed by LINSTOR.
- `linstor_resource_state`: State of each resource managed by LINSTOR.
- `linstor_volume_state`: State of each volume managed by LINSTOR.
- `linstor_volume_allocated_size_bytes`: Total storage in bytes currently allocated for given volume.
- `linstor_storage_pool_capacity_free_bytes`: Total free storage in bytes available in given LINSTOR storage-pool.
- `linstor_storage_pool_capacity_total_bytes`: Total storage in bytes managed by given LINSTOR storage-pool.
- `linstor_storage_pool_error_count`: Number or errors logged on given LINSTOR storage-pool.
- `linstor_error_reports_count`: Number or error-reports logged by LINSTOR.
- `linstor_scrape_requests_count`: Number of scrape requests on the LINSTOR metrics endpoint since last restart.
- `linstor_scrape_duration_seconds`: Time spent scraping LINSTOR metrics in seconds.

## LINSTOR JVM Metrics

- `jvm_memory_bytes_used`: Used bytes of a given JVM memory area.
- `jvm_memory_bytes_committed`: Committed bytes of a given JVM memory area.
- `jvm_memory_bytes_max`: Max bytes of a given JVM memory area.
- `jvm_memory_bytes_init`: Initial bytes of a given JVM memory area.
- `jvm_memory_pool_bytes_used`: Used bytes of a given JVM memory pool.
- `jvm_memory_pool_bytes_committed`: Committed bytes of a given JVM memory pool.
- `jvm_memory_pool_bytes_max`: Max bytes of a given JVM memory pool.
- `jvm_memory_pool_bytes_init`: Initial bytes of a given JVM memory pool.
- `jvm_classes_loaded`: Number of classes currently loaded in the JVM.
- `jvm_classes_loaded_total`: Number of classes that have been loaded since JVM start.
- `jvm_classes_unloaded_total`: Number of classes that have been unloaded since JVM start.
- `jvm_threads_current`: Current thread count of a JVM.
- `jvm_threads_daemon`: Daemon thread count of a JVM.
- `jvm_threads_peak`: Peak thread count of a JVM.
- `jvm_threads_started_total`: Started thread count of a JVM.
- `jvm_threads_deadlocked`: Cycles of JVM threads that are in a deadlock state.
- `jvm_threads_deadlocked_monitor`: Cycles of JVM threads that are in a deadlock state waiting to acquire object monitors.
- `jvm_threads_state`: Number of threads by state.
- `jvm_info`: JVM version info.
- `process_cpu_seconds_total`: Total user and system CPU time spent in seconds.
- `process_start_time_seconds`: Start time of the process since Unix epoch in seconds.
- `process_open_fds`: Number of open file descriptors.
- `process_max_fds`: Maximum number of open file descriptors.
- `process_virtual_memory_bytes`: Virtual memory size in bytes.
- `process_resident_memory_bytes`: Resident memory size in bytes.
- `jvm_buffer_pool_used_bytes`: Used bytes of a given JVM buffer pool.
- `jvm_buffer_pool_capacity_bytes`: Capacity of JVM buffer pool in bytes.
- `jvm_buffer_pool_used_buffers`: Used buffers of a given JVM buffer pool.
- `jvm_memory_pool_allocated_bytes_total`: Total bytes allocated in given JVM memory pool.
- `jvm_gc_collection_seconds`: Time spent in a given JVM garbage collector in seconds.
