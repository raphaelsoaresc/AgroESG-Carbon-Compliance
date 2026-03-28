'use client';
import { useEffect, useRef } from 'react';

declare global {
  interface Window {
    MercadoPago: any;
  }
}

interface PaymentBrickProps {
  preferenceId: string;
  onPaymentSuccess?: () => void;
}

export default function PaymentBrick({ preferenceId, onPaymentSuccess }: PaymentBrickProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const brickController = useRef<any>(null);

  useEffect(() => {
    // CAPTURA O VALOR ATUAL PARA UMA VARIÁVEL LOCAL (SNAPSHOT)
    // Isso garante ao TS que o valor não mudará para null durante a execução
    const container = containerRef.current;
    const publicKey = process.env.NEXT_PUBLIC_MP_PUBLIC_KEY;

    if (!window.MercadoPago || !preferenceId || !publicKey || !container) return;

    const initBrick = async () => {
      // Aqui usamos a variável 'container' em vez de 'containerRef.current'
      container.innerHTML = '';

      try {
        const mp = new window.MercadoPago(publicKey, { locale: 'pt-BR' });
        const bricksBuilder = mp.bricks();

        brickController.current = await bricksBuilder.create(
          'payment', 
          'paymentBrick_container', 
          {
            initialization: {
              amount: 150,
              preferenceId: preferenceId,
            },
            customization: {
              visual: { theme: 'default' },
              paymentMethods: {
                ticket: 'all',
                bankTransfer: 'all',
                creditCard: 'all',
              },
            },
            callbacks: {
              onReady: () => console.log("Mercado Pago: Pronto"),
              onSubmit: ({ formData }: any) => {
                console.log("Dados:", formData);
                if (onPaymentSuccess) onPaymentSuccess();
              },
              onError: (error: any) => console.error("Erro Brick:", error),
            },
          }
        );
      } catch (err) {
        console.error("Falha ao carregar o checkout:", err);
      }
    };

    initBrick();

    return () => {
      if (brickController.current) {
        brickController.current.unmount();
        brickController.current = null;
      }
      // Usamos a variável capturada 'container' no cleanup também
      if (container) container.innerHTML = '';
    };
  }, [preferenceId, onPaymentSuccess]);

  return (
    <div className="bg-white p-6 md:p-10 rounded-[2rem] shadow-2xl border border-slate-100 w-full max-w-xl mx-auto">
      <div className="text-center mb-8">
        <h3 className="text-2xl font-black text-slate-900 uppercase tracking-tight">
          Finalizar Pagamento Seguro
        </h3>
        <p className="text-slate-500 text-sm font-medium mt-2">Liberação imediata após o pagamento</p>
      </div>

      <div 
        id="paymentBrick_container" 
        ref={containerRef} 
        className="w-full min-h-[600px]"
      ></div>
      
      <div className="flex items-center justify-center gap-2 mt-8 pt-6 border-t border-slate-100">
        <span className="text-[10px] text-slate-400 font-black uppercase tracking-[0.2em]">
          🔒 Pagamento processado pelo Mercado Pago
        </span>
      </div>
    </div>
  );
}
