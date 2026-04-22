# Portfolio Intelligence System

A system that analyzes financial markets and manages client portfolios. It is designed to track key market indicators such as the Nifty 50, Gold, and Silver.

## Installation

1. Create a virtual environment (optional but recommended):
```bash
python -m venv venv
source venv/bin/activate  # On macOS/Linux
# or `venv\Scripts\activate` on Windows
```

2. Install the required dependencies:
```bash
pip install -r requirements.txt
```

## Running the System

### Backend Application (FastAPI)
To run the backend API server:
```bash
uvicorn backend.main:app --reload
```
The FastAPI application will be accessible at [http://127.0.0.1:8000](http://127.0.0.1:8000).
Interactive API documentation will be at [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs).

### Dashboard (Streamlit)
To run the interactive dashboard:
```bash
streamlit run dashboard/dashboard.py
```
The Streamlit dashboard will be accessible at [http://localhost:8501](http://localhost:8501).
