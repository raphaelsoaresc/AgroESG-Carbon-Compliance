from datetime import datetime, timedelta
from typing import Optional
from fastapi import Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from supabase import create_client, Client
from pydantic_settings import BaseSettings, SettingsConfigDict
from schemas import ComplianceResponse

# Configurações específicas de Billing e Auth
class BillingSettings(BaseSettings):
    supabase_url: str
    supabase_key: str
    mp_access_token: str
    
    model_config = SettingsConfigDict(
        env_file=".env", 
        extra="ignore",
        env_prefix="",       
        case_sensitive=False 
    )

settings = BillingSettings()
supabase: Client = create_client(settings.supabase_url, settings.supabase_key)

# Esquema de segurança para ler o Token JWT do cabeçalho
security = HTTPBearer(auto_error=False)

ADMIN_EMAIL = "rsdcruz97@gmail.com"

DEMO_IDS =[
    'PA-1505304-BB9F3EB9FBCD498BB2F07BB62EE58B4E',
    'AM-1301704-5F6515F5B115419F9114891B2D3A2C54',
    'RO-1100205-F79D65912A3643AEA543049332BD109F',
    'MT-5101605-503D1E16017740379EE73DB43DE1358C'
]

async def get_current_user(credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)) -> Optional[str]:
    if not credentials:
        return None
    try:
        user_response = supabase.auth.get_user(credentials.credentials)
        if user_response and user_response.user:
            return user_response.user.email
    except Exception:
        return None
    return None

def check_user_permission(email: str, car_id: str) -> bool:
    # REGRA 0: Admin bypass
    if email == ADMIN_EMAIL:
        return True

    # Check 1: Compra avulsa
    purchase = supabase.table("single_purchases").select("id").eq("email", email).eq("car_id", car_id).execute()
    if purchase.data and len(purchase.data) > 0:
        return True

    # Check 2: Plano PRO e Limite de 50
    profile = supabase.table("profiles").select("plan_type").eq("email", email).execute()
    
    if profile.data and len(profile.data) > 0:
        if profile.data[0].get("plan_type") == "pro":
            start_of_period = (datetime.now() - timedelta(days=30)).isoformat()
            usage = supabase.table("usage_logs").select("id").eq("email", email).gt("queried_at", start_of_period).execute()
            
            if usage.data is not None and len(usage.data) < 50:
                return True
                
    return False

def register_usage(email: str, car_id: str):
    supabase.table("usage_logs").insert({"email": email, "car_id": car_id}).execute()

def censor_response(res: ComplianceResponse) -> ComplianceResponse:
    res.financial_liabilities = None
    res.environmental_score = None
    res.deforestation_metrics = None
    res.social_score = None
    res.risk_analysis = None
    return res