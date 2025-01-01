#!/bin/bash

cd "$(dirname "$0")"

pkill -f 'python.*dash_app.py' 2>/dev/null

if [ ! -d "venv" ]; then
    echo "Virtual environment not found. Running setup..."
    ./setup_env.sh
fi

echo "Activating virtual environment..."
source venv/bin/activate

echo "Running the Dash app..."
python dash_app.py &

PID=$!

sleep 1

xdg-open http://127.0.0.1:8050/ &

wait $PID

echo "Deactivating virtual environment..."
deactivate
