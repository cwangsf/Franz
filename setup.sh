#!/bin/bash

# Kafka Learning Project Setup Script

echo "🚀 Kafka Learning Project Setup"
echo "================================"

# Check Python version
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is not installed. Please install Python 3.8 or higher."
    exit 1
fi

echo "✓ Python 3 found: $(python3 --version)"

# Create virtual environment
echo ""
echo "📦 Creating virtual environment..."
python3 -m venv venv
source venv/bin/activate

# Install dependencies
echo "📦 Installing dependencies..."
pip install -r requirements.txt

# Copy environment file
if [ ! -f .env ]; then
    echo "⚙️  Creating .env file..."
    cp .env.example .env
fi

echo ""
echo "✓ Setup complete!"
echo ""
echo "📋 Next steps:"
echo "   1. Activate virtual environment: source venv/bin/activate"
echo "   2. Start Kafka (if not already running)"
echo "   3. Run examples:"
echo "      - Basic producer: python 1_basic_producer.py"
echo "      - Basic consumer: python 2_basic_consumer.py"
echo "      - Stream processing: python 3_stream_processing.py"
echo "      - Microservices demo: See README.md for multi-terminal setup"
echo ""
echo "📖 Read README.md for complete documentation"
