import { NextResponse } from 'next/server';

export async function GET(request, { params }) {
  try {
    // 1. Garante que os params foram lidos (Next.js 15+)
    const resolvedParams = await params;
    // Tenta pegar 'car' ou 'id' (ajuste conforme o nome da sua pasta [car] ou [id])
    const carId = resolvedParams.car || resolvedParams.id;

    if (!carId) {
      return NextResponse.json({ error: 'ID do imóvel não fornecido' }, { status: 400 });
    }

    // 2. Verifica se as variáveis de ambiente existem antes de usar
    const rawBaseUrl = process.env.CAIPORA_API_URL || process.env.NEXT_PUBLIC_API_URL;
    const apiKey = process.env.CAIPORA_API_KEY || process.env.NEXT_PUBLIC_API_KEY;

    if (!rawBaseUrl) {
      console.error("❌ ERRO: Variável CAIPORA_API_URL não definida!");
      return NextResponse.json({ error: 'Configuração do servidor incompleta (URL)' }, { status: 500 });
    }

    const baseUrl = rawBaseUrl.replace(/\/$/, "");
    const apiUrl = `${baseUrl}/compliance/car/${carId}`;

    console.log("🚀 Chamando API original em:", apiUrl);

    const response = await fetch(apiUrl, {
      method: 'GET',
      headers: {
        'X-API-Key': apiKey || '',
        'Content-Type': 'application/json',
      },
      // Evita que o Next.js use cache velho
      cache: 'no-store'
    });

    // 3. Verifica se a resposta é OK antes de tentar ler o JSON
    if (!response.ok) {
      const errorText = await response.text();
      console.error(`❌ Backend retornou erro ${response.status}:`, errorText);
      return NextResponse.json(
        { error: 'Erro no backend', details: errorText }, 
        { status: response.status }
      );
    }

    const data = await response.json();
    return NextResponse.json(data);

  } catch (error) {
    console.error("💥 Erro fatal no Proxy do Front-end:", error);
    return NextResponse.json(
      { error: 'Falha interna no servidor do front', message: error.message }, 
      { status: 500 }
    );
  }
}