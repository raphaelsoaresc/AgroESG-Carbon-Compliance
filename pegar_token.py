from supabase import create_client

# 1. Coloque suas chaves do Supabase aqui
url = "https://pxajvdjaszvaffibwulm.supabase.co"
key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InB4YWp2ZGphc3p2YWZmaWJ3dWxtIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzM3MDEyMTIsImV4cCI6MjA4OTI3NzIxMn0.EjTt1Ir_oImyhIEEol3N6mkTYmYIWLxGOphuJSZFWfU"
supabase = create_client(url, key)

# 2. Coloque o e-mail e senha do usuário que você criou lá no painel do Supabase
resposta = supabase.auth.sign_in_with_password({
    "email": "rsdcruz97@gmail.com",
    "password": "Rmbn@32364301"
})

print("\nCOPIE O TOKEN ABAIXO:\n")
print(resposta.session.access_token)