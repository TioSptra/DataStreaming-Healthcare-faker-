from pyflink.table import EnvironmentSettings, StreamTableEnvironment

BOOTSTRAP = "broker:29092"
SCHEMA_REGISTRY = "http://schema-registry:8081"
SOURCE_TOPIC = "treatment_data"
SINK_TOPIC = "payment_data"

def main():
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = StreamTableEnvironment.create(environment_settings=env_settings)

    t_env.get_config().set("pipeline.name", "Treatment Tumbling Window Payment Aggregation ")
    t_env.get_config().set("table.exec.source.idle-timeout", "10 s")

    t_env.execute_sql(f"""
        CREATE TABLE treatment_src (
            treatment_id        STRING,
            patient_id          STRING,
            `timestamp`         STRING,
            age                 INTEGER,
            gender              STRING,
            blood_type          STRING,
            diagnosis           STRING,
            doctor              STRING,
            room                STRING,
            payment_method      STRING,
            amount              DOUBLE,
            rating              INTEGER,
            outcome             STRING,
            event_ts TIMESTAMP(3) METADATA FROM 'timestamp',
            WATERMARK FOR event_ts AS event_ts - INTERVAL '10' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{SOURCE_TOPIC}',
            'properties.bootstrap.servers' = '{BOOTSTRAP}',
            'properties.group.id' = 'pyflink-agg-60s',
            'scan.startup.mode' = 'earliest-offset',
            'value.format' = 'avro-confluent',
            'value.avro-confluent.schema-registry.url' = '{SCHEMA_REGISTRY}',
            'properties.allow.auto.create.topics' = 'true'
        )
    """)

    t_env.execute_sql(f"""
        CREATE TABLE payment_agg_sink (
            window_start    TIMESTAMP(3),
            window_end      TIMESTAMP(3),
            payment_method  STRING,
            payment_count   BIGINT,
            total_amount    DOUBLE,
            avg_amount      DOUBLE
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{SINK_TOPIC}',
            'properties.bootstrap.servers' = '{BOOTSTRAP}',
            'value.format' = 'avro-confluent',
            'value.avro-confluent.schema-registry.url' = '{SCHEMA_REGISTRY}',
            'sink.partitioner' = 'round-robin',
            'properties.allow.auto.create.topics' = 'true'
        )
    """)

    t_env.execute_sql("""
        INSERT INTO payment_agg_sink
        SELECT
            window_start,
            window_end,
            payment_method,
            COUNT(*)                    AS payment_count,
            CAST(SUM(amount) AS DOUBLE) AS total_amount,
            CAST(AVG(amount) AS DOUBLE) AS avg_amount
        FROM TABLE(
            TUMBLE(TABLE treatment_src, DESCRIPTOR(event_ts), INTERVAL '60' SECOND)
        )
        GROUP BY window_start, window_end, payment_method
    """)

if __name__ == "__main__":
    main()
