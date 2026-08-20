import mlflow.pyfunc
import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from prometheus_fastapi_instrumentator import Instrumentator

app = FastAPI(title="KrediRadar API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

Instrumentator().instrument(app).expose(app, endpoint="/metrics")

model = None


@app.on_event("startup")
def load_model():
    global model
    model = mlflow.pyfunc.load_model("model_export")


class CreditApplication(BaseModel):
    age: int = Field(..., alias="Age", ge=18, le=100)
    job: int = Field(..., alias="Job", ge=0, le=3, description="0=unskilled ... 3=highly skilled")
    sex: str = Field(..., alias="Sex", examples=["male", "female"])
    housing: str = Field(..., alias="Housing", examples=["own", "rent", "free"])
    saving_accounts: str = Field(..., alias="Saving accounts", examples=["little", "moderate", "quite rich", "rich", "no_account"])
    checking_account: str = Field(..., alias="Checking account", examples=["little", "moderate", "rich", "no_account"])
    credit_amount: float = Field(..., alias="Credit amount", gt=0)
    duration: int = Field(..., alias="Duration", gt=0)
    purpose: str = Field(..., alias="Purpose", examples=["car", "radio/TV", "furniture/equipment", "business", "education"])

    class Config:
        populate_by_name = True


def add_derived_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["monthly_burden"] = df["Credit amount"] / df["Duration"]
    df["age_group"] = pd.cut(
        df["Age"], bins=[0, 25, 40, 60, 100],
        labels=["genc", "orta_genc", "orta_yasli", "yasli"]
    )
    return df


@app.get("/health")
def health():
    return {"status": "ok", "model_loaded": model is not None}


@app.post("/predict")
def predict(application: CreditApplication):
    if model is None:
        raise HTTPException(status_code=503, detail="Model yuklenemedi")

    raw = pd.DataFrame([application.dict(by_alias=True)])
    enriched = add_derived_features(raw)

    prediction = model.predict(enriched)
    result = int(prediction[0]) if hasattr(prediction[0], "item") else prediction[0]

    return {
        "prediction": result,
        "label": "iyi risk" if result == 1 else "kotu risk"
    }
