import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings


def create_kafka_source(t_env):
    kafka_key = os.environ.get("KAFKA_WEB_TRAFFIC_KEY", "")
    kafka_secret = os.environ.get("KAFKA_WEB_TRAFFIC_SECRET", "")
    topic = os.environ.get("KAFKA_TOPIC", "bootcamp-events-prod")
    bootstrap_servers = os.environ.get("KAFKA_URL", "pkc-rgm37.us-west-2.aws.confluent.cloud:9092")
    group_id = os.environ.get("KAFKA_GROUP", "web-events")

    # Format waktu ISO 8601 seperti "2025-10-29T14:30:45.123Z"
    pattern = "yyyy-MM-dd''T''HH:mm:ss.SSS''Z''"
    ddl = f"""
        CREATE TABLE web_events (
            ip STRING,
            event_time STRING,
            host STRING,
            ts AS TO_TIMESTAMP(event_time, '{pattern}'),
            WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{topic}',
            'properties.bootstrap.servers' = '{bootstrap_servers}',
            'properties.group.id' = '{group_id}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'PLAIN',
            'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{kafka_key}\" password=\"{kafka_secret}\";',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json'
        )
    """
    t_env.execute_sql(ddl)


def create_session_detail_sink(t_env):
    ddl = f"""
        CREATE TABLE session_detail (
            ip STRING,
            host STRING,
            session_start TIMESTAMP(3),
            session_end TIMESTAMP(3),
            event_count BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = 'session_detail',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        )
    """
    t_env.execute_sql(ddl)


def sessionize_to_detail_table():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(3)
    env.enable_checkpointing(10000)  # 10 detik

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    create_kafka_source(t_env)
    create_session_detail_sink(t_env)

    # Session window dengan gap 5 menit
    query = """
        INSERT INTO session_detail
        SELECT
            ip,
            host,
            SESSION_START(ts, INTERVAL '5' MINUTE) AS session_start,
            SESSION_END(ts, INTERVAL '5' MINUTE) AS session_end,
            COUNT(*) AS event_count
        FROM web_events
        GROUP BY
            ip,
            host,
            SESSION(ts, INTERVAL '5' MINUTE)
    """

    print("Starting sessionization job...")
    t_env.execute_sql(query).wait()


if __name__ == "__main__":
    sessionize_to_detail_table()