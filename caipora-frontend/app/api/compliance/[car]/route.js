import { NextResponse } from 'next/server';

export async function GET(request, { params }) {
  const { car } = await params; 
  const carId = car;
  
  // 1. Verifique se não há uma barra sobrando no final da CAIPORA_API_URL no .env.local
  const baseUrl = process.env.CAIPORA_API_URL.replace(/\/$/, ""); 
  
  // 2. Monte a URL (Verifique se o seu FastAPI usa /v1/compliance ou apenas /compliance)
  const apiUrl = `${baseUrl}/compliance/car/${carId}`;

  // LOG DE DEBUG - Isso vai aparecer no seu terminal (onde roda o npm run dev)
  console.log("🚀 Chamando API original em:", apiUrl);

  try {
    const response = await fetch(apiUrl, {
      headers: {
        'X-API-Key': process.env.CAIPORA_API_KEY,
        'Content-Type': 'application/json',
      },
    });

    const data = await response.json();
    console.log("📦 Resposta da API:", data); // Ver o que a API retornou
    
    return NextResponse.json(data);
  } catch (error) {
    return NextResponse.json({ error: 'Falha na conexão' }, { status: 500 });
  }
}