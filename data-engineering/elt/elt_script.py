import socket
import time
import subprocess
import os

def wait_for_service(host, port=5432, max_retries=60, delay_seconds=2):
    """
    Wait until a TCP connection to host:port is possible.
    In Docker Compose, this is a simple way to wait for Postgres containers
    to start accepting connections on port 5432.
    """
    for attempt in range(1, max_retries + 1):
        try:
            # If this succeeds, something is listening on host:port
            with socket.create_connection((host, port), timeout=2):
                print(f"Service at {host}:{port} is available.")
                return True
        except OSError as e:
            print(f"[{attempt}/{max_retries}] {host}:{port} not available -> {e}")
            time.sleep(delay_seconds)
    return False


# Wait for both DBs to be reachable inside the Docker network.
# Note: depends_on in docker-compose does NOT guarantee DB readiness.
if not wait_for_service("source_postgres", 5432):
    raise SystemExit("source_postgres not reachable")

if not wait_for_service("destination_postgres", 5432):
    raise SystemExit("destination_postgres not reachable")

print("Starting ELT process...")

# Source database connection details (must match docker-compose env vars)
source_config = {
    "dbname": "source_db",
    "user": "source_user",
    "password": "source_password",
    "host": "source_postgres",  # Docker Compose service name (DNS)
    "port": 5432,               # Container port (inside Docker network)
}

# Destination database connection details (must match docker-compose env vars)
target_config = {
    "dbname": "dest_db",
    "user": "dest_user",
    "password": "dest_password",
    "host": "destination_postgres",  # Docker Compose service name (DNS)
    "port": 5432,                    # Container port (inside Docker network)
}

# Where to store the dump file inside the ELT container.
# /tmp is always available and doesn't require volume mounts.
dump_file = "/tmp/dump.sql"

# -------------------------
# 1) DUMP from source
# -------------------------
dump_env = os.environ.copy()
dump_env["PGPASSWORD"] = source_config["password"]  # Avoid interactive password prompt

dump_command = [
    "pg_dump",
    "-h", source_config["host"],
    "-p", str(source_config["port"]),
    "-U", source_config["user"],
    "-d", source_config["dbname"],
    # Important for cross-environment restores:
    # - don't restore ownership/roles from source to destination
    "--no-owner",
    "--no-acl",
    # Make reruns idempotent:
    # - clean drops existing objects
    # - if-exists prevents errors when objects don't exist yet
    "--if-exists",
    "--clean",
    "-f", dump_file,
]

# check=True makes Python raise an error if pg_dump fails (good for pipelines)
subprocess.run(dump_command, env=dump_env, check=True)

# -------------------------
# 2) LOAD into destination
# -------------------------
load_env = os.environ.copy()
load_env["PGPASSWORD"] = target_config["password"]

load_command = [
    "psql",
    "-h", target_config["host"],
    "-p", str(target_config["port"]),
    "-U", target_config["user"],
    "-d", target_config["dbname"],
    "-f", dump_file,
]

subprocess.run(load_command, env=load_env, check=True)

print("ELT process completed successfully.")
