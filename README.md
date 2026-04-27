# Delhivery Hackathon

Two products: Commerce AI + Project Echo.

See commerce_ai/README.md for full details.

## Quick Start

    cd commerce_ai
    pip install -e .[dev]
    python -m data.generate_sample_data
    uvicorn api.app:app --port 8000

    # Frontend
    cd commerce_ai/frontend
    npm install
    npm run dev

Open http://localhost:5173
