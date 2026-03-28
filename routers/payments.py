from fastapi import APIRouter, Request, HTTPException
from pydantic import BaseModel
import mercadopago
from services.billing import settings, supabase

router = APIRouter(tags=["Payments"])

sdk = mercadopago.SDK(settings.mp_access_token)

class SubscriptionRequest(BaseModel):
    email: str

class PreferenceRequest(BaseModel):
    car_id: str
    email: str

@router.post("/payments/create-preference")
async def create_preference(payload: PreferenceRequest):
    try:
        preference_data = {
            "items":[{
                "title": f"Laudo Caipora: {payload.car_id}", 
                "quantity": 1, 
                "unit_price": 150.00, 
                "currency_id": "BRL"
            }],
            "payer": {"email": payload.email},
            "external_reference": payload.car_id,
            "back_urls": {"success": "https://agrimarketintel.com/caipora?status=success"},
            "auto_return": "approved",
        }
        res = sdk.preference().create(preference_data)
        return {
            "id": res["response"]["id"],
            "init_point": res["response"]["init_point"]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/payments/create-subscription")
async def create_subscription(payload: SubscriptionRequest):
    try:
        subscription_data = {
            # 1. ATUALIZADO: Nome do plano e quantidade de consultas
            "reason": "Plano Pro Caipora Sentinela (50 consultas/mês)",
            "external_reference": payload.email,
            "payer_email": payload.email,
            "auto_recurring": {
                "frequency": 1,
                "frequency_type": "months",
                # 2. ATUALIZADO: Valor reduzido para o limite permitido
                "transaction_amount": 4000.00, 
                "currency_id": "BRL"
            },
            "back_url": "https://agrimarketintel.com/caipora",
            "status": "pending"
        }
        result = sdk.preapproval().create(subscription_data)
        
        if result["status"] >= 400:
            raise HTTPException(status_code=result["status"], detail=result["response"])
            
        return {"init_point": result["response"]["init_point"]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/payments/webhook")
async def mp_webhook(request: Request):
    try:
        data = await request.json()
        if data.get("type") == "payment":
            payment_id = data["data"]["id"]
            payment_info = sdk.payment().get(payment_id)["response"]
            
            if payment_info["status"] == "approved":
                supabase.table("single_purchases").insert({
                    "car_id": payment_info["external_reference"],
                    "email": payment_info["payer"]["email"],
                    "mp_payment_id": str(payment_id)
                }).execute()
        return {"status": "ok"}
    except:
        return {"status": "error"}