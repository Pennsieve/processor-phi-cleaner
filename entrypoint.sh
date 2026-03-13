#!/bin/sh

# Create agent config from environment variables
if [ -n "$PENNSIEVE_API_KEY" ] && [ -n "$PENNSIEVE_API_SECRET" ]; then
    mkdir -p ~/.pennsieve
    cat > ~/.pennsieve/config.ini <<CONF
[agent]
port=9000
upload_chunk_size=32
upload_workers=10

[default]
api_token=${PENNSIEVE_API_KEY}
api_secret=${PENNSIEVE_API_SECRET}
CONF
    echo "Pennsieve agent config created."
fi

# Start Pennsieve Agent in the background (needed for file uploads)
if command -v pennsieve >/dev/null 2>&1; then
    echo "Starting Pennsieve Agent..."
    # Unset API host so the agent uses its .io defaults instead of
    # hardcoding api2.pennsieve.net when PENNSIEVE_API_HOST is set.
    env -u PENNSIEVE_API_HOST -u PENNSIEVE_API_HOST2 pennsieve agent start &
    AGENT_PID=$!
    # Wait for agent gRPC server on port 9000
    for i in $(seq 1 30); do
        if nc -z localhost 9000 2>/dev/null; then
            echo "Pennsieve Agent ready on port 9000."
            break
        fi
        echo "  Waiting for agent (attempt $i/30)..."
        sleep 1
    done
fi

if [ -n "$AWS_LAMBDA_RUNTIME_API" ]; then
    exec python -m awslambdaric handler.handler
else
    exec "$@"
fi
