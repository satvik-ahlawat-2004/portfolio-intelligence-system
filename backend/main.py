from fastapi import FastAPI

app = FastAPI(title="Portfolio Intelligence System API")

@app.get("/")
def read_root():
    return {"message": "Welcome to the Portfolio Intelligence System API"}

@app.get("/health")
def health_check():
    return {"status": "healthy"}
