import os
import logging
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

import requests
from fastapi import FastAPI, HTTPException, Header, Query
from fastapi.middleware.cors import CORSMiddleware
import firebase_admin
from firebase_admin import credentials, firestore
from pydantic import BaseModel

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if not firebase_admin._apps:
    creds_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    if creds_json:
        import json
        cred = credentials.Certificate(json.loads(creds_json))
        firebase_admin.initialize_app(cred, options={"projectId": os.getenv("GOOGLE_CLOUD_PROJECT", "dashmeta-aba7d")})
    else:
        firebase_admin.initialize_app(options={"projectId": os.getenv("GOOGLE_CLOUD_PROJECT", "dashmeta-aba7d")})

db = firestore.client()

# Inicializar FastAPI
app = FastAPI(title="Meta Grafana Dashboard API", version="1.0.0")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configurações
META_ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN")
META_AD_ACCOUNT_ID = os.getenv("META_AD_ACCOUNT_ID", "act_1934031877437397")
API_KEY = os.getenv("API_KEY", "change-me-in-production")
ACTION_TYPE = "onsite_conversion.messaging_conversation_started_7d"
META_API_VERSION = "v20.0"

# Modelos
class HealthResponse(BaseModel):
    status: str
    timestamp: str

class MetricItem(BaseModel):
    id: str
    name: str
    campaign_id: Optional[str] = None
    adset_id: Optional[str] = None
    spend_today: float
    conv_today: int
    cpl_today: Optional[float]
    spend_30m: float
    conv_30m: int
    cpl_30m: Optional[float]

# Autenticação
def verify_api_key(x_api_key: str = Header(...)):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API Key")

# Funções auxiliares Meta API
def get_meta_insights(level: str, date_preset: str = "today") -> List[Dict]:
    """Busca insights da Meta API"""
    url = f"https://graph.facebook.com/{META_API_VERSION}/{META_AD_ACCOUNT_ID}/insights"
    
    fields = "spend,actions,cost_per_action_type"
    if level == "campaign":
        fields = f"campaign_id,campaign_name,{fields}"
    elif level == "adset":
        fields = f"campaign_id,campaign_name,adset_id,adset_name,{fields}"
    elif level == "ad":
        fields = f"campaign_id,adset_id,ad_id,ad_name,{fields}"
    
    params = {
        "access_token": META_ACCESS_TOKEN,
        "level": level,
        "date_preset": date_preset,
        "fields": fields,
        "use_unified_attribution_setting": "true",
        "limit": 500
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        return data.get("data", [])
    except Exception as e:
        logger.error(f"Erro ao buscar insights {level}: {e}")
        raise HTTPException(status_code=500, detail=f"Erro Meta API: {str(e)}")

def extract_action_value(actions: List[Dict], action_type: str) -> int:
    """Extrai valor de uma ação específica"""
    if not actions:
        return 0
    for action in actions:
        if action.get("action_type") == action_type:
            return int(float(action.get("value", 0)))
    return 0

def extract_cost_per_action(cost_per_action_type: List[Dict], action_type: str) -> Optional[float]:
    """Extrai custo por ação específica"""
    if not cost_per_action_type:
        return None
    for item in cost_per_action_type:
        if item.get("action_type") == action_type:
            return float(item.get("value", 0))
    return None

def calculate_cpl(spend: float, conv: int, cpa: Optional[float]) -> Optional[float]:
    """Calcula CPL (prefere CPA da API, fallback para cálculo manual)"""
    if cpa is not None:
        return cpa
    if conv > 0:
        return spend / conv
    return None

# Endpoints
@app.get("/", response_model=HealthResponse)
async def root():
    return HealthResponse(
        status="ok",
        timestamp=datetime.now(timezone.utc).isoformat()
    )

@app.get("/health", response_model=HealthResponse)
async def health():
    return HealthResponse(
        status="healthy",
        timestamp=datetime.now(timezone.utc).isoformat()
    )

@app.post("/collect")
async def collect(x_api_key: str = Header(...)):
    """Coleta dados da Meta API e grava snapshot no Firestore"""
    verify_api_key(x_api_key)
    
    try:
        now = datetime.now(timezone.utc)
        doc_id = now.strftime("%Y%m%d_%H%M")
        
        # Buscar dados
        campaigns = get_meta_insights("campaign")
        adsets = get_meta_insights("adset")
        ads = get_meta_insights("ad")
        
        # Processar dados
        snapshot = {
            "ts": now.isoformat(),
            "date": now.strftime("%Y-%m-%d"),
            "campaign": [],
            "adset": [],
            "ad": []
        }
        
        for item in campaigns:
            spend = float(item.get("spend", 0))
            actions = item.get("actions", [])
            cost_per_action = item.get("cost_per_action_type", [])
            
            conv = extract_action_value(actions, ACTION_TYPE)
            cpa = extract_cost_per_action(cost_per_action, ACTION_TYPE)
            cpl = calculate_cpl(spend, conv, cpa)
            
            snapshot["campaign"].append({
                "id": item.get("campaign_id"),
                "name": item.get("campaign_name", ""),
                "spend": spend,
                "conv": conv,
                "cpl": cpl
            })
        
        for item in adsets:
            spend = float(item.get("spend", 0))
            actions = item.get("actions", [])
            cost_per_action = item.get("cost_per_action_type", [])
            
            conv = extract_action_value(actions, ACTION_TYPE)
            cpa = extract_cost_per_action(cost_per_action, ACTION_TYPE)
            cpl = calculate_cpl(spend, conv, cpa)
            
            snapshot["adset"].append({
                "id": item.get("adset_id"),
                "name": item.get("adset_name", ""),
                "campaign_id": item.get("campaign_id"),
                "spend": spend,
                "conv": conv,
                "cpl": cpl
            })
        
        for item in ads:
            spend = float(item.get("spend", 0))
            actions = item.get("actions", [])
            cost_per_action = item.get("cost_per_action_type", [])
            
            conv = extract_action_value(actions, ACTION_TYPE)
            cpa = extract_cost_per_action(cost_per_action, ACTION_TYPE)
            cpl = calculate_cpl(spend, conv, cpa)
            
            snapshot["ad"].append({
                "id": item.get("ad_id"),
                "name": item.get("ad_name", ""),
                "campaign_id": item.get("campaign_id"),
                "adset_id": item.get("adset_id"),
                "spend": spend,
                "conv": conv,
                "cpl": cpl
            })
        
        # Salvar no Firestore
        db.collection("intraday_snapshots").document(doc_id).set(snapshot)
        
        logger.info(f"Snapshot {doc_id} salvo: {len(campaigns)} campaigns, {len(adsets)} adsets, {len(ads)} ads")
        
        return {
            "status": "success",
            "doc_id": doc_id,
            "counts": {
                "campaigns": len(campaigns),
                "adsets": len(adsets),
                "ads": len(ads)
            }
        }
    
    except Exception as e:
        logger.error(f"Erro no collect: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/intraday/{level}")
async def get_intraday(level: str, x_api_key: str = Header(...)):
    """Retorna dados intraday com últimos 30min calculados"""
    verify_api_key(x_api_key)
    
    if level not in ["campaign", "adset", "ad"]:
        raise HTTPException(status_code=400, detail="Level deve ser campaign, adset ou ad")
    
    try:
        # Buscar os 2 snapshots mais recentes
        snapshots = db.collection("intraday_snapshots").order_by("ts", direction=firestore.Query.DESCENDING).limit(2).stream()
        
        snapshot_list = [snap.to_dict() for snap in snapshots]
        
        if len(snapshot_list) == 0:
            return []
        
        current = snapshot_list[0]
        previous = snapshot_list[1] if len(snapshot_list) > 1 else None
        
        # Processar dados
        result = []
        current_items = {item["id"]: item for item in current.get(level, [])}
        previous_items = {item["id"]: item for item in previous.get(level, [])} if previous else {}
        
        for item_id, curr in current_items.items():
            prev = previous_items.get(item_id, {"spend": 0, "conv": 0})
            
            spend_30m = curr["spend"] - prev["spend"]
            conv_30m = curr["conv"] - prev["conv"]
            cpl_30m = (spend_30m / conv_30m) if conv_30m > 0 else None
            
            result.append({
                "id": item_id,
                "name": curr["name"],
                "campaign_id": curr.get("campaign_id"),
                "adset_id": curr.get("adset_id"),
                "spend_today": curr["spend"],
                "conv_today": curr["conv"],
                "cpl_today": curr["cpl"],
                "spend_30m": spend_30m,
                "conv_30m": conv_30m,
                "cpl_30m": cpl_30m
            })
        
        return result
    
    except Exception as e:
        logger.error(f"Erro no intraday {level}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
