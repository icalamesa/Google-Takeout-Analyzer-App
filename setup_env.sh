#!/bin/bash

cd "$(dirname "$0")"

update_dependencies() {
    echo "Activating virtual environment..."
    source venv/bin/activate || { echo "Failed to activate virtual environment"; exit 1; }

    echo "Updating pip..."
    pip install --upgrade pip || { echo "Failed to upgrade pip"; exit 1; }

    echo "Installing/updating dependencies..."
    pip install -r requirements.txt || { echo "Failed to install dependencies"; exit 1; }

    echo "Dependencies installed/updated."
}

if [[ "$1" == "--update-dependencies" ]]; then
    if [ ! -d "venv" ]; then
        echo "No virtual environment found. Creating one..."
        python3 -m venv venv || { echo "Failed to create virtual environment"; exit 1; }
    fi

    update_dependencies
    exit 0
fi

if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv || { echo "Failed to create virtual environment"; exit 1; }

    update_dependencies

    echo "Setup complete. Virtual environment created and dependencies installed."
else
    echo "Virtual environment already exists. Activating..."
    source venv/bin/activate || { echo "Failed to activate virtual environment"; exit 1; }
fi

echo "Virtual environment is now active. You can run your Dash app."
echo "Type 'deactivate' to exit the virtual environment."
