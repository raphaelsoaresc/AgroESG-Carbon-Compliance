import type { Metadata } from "next";
import Script from "next/script";

export const metadata: Metadata = {
  title: "Caipora Sentinela | Compliance Geoespacial",
  description: "Acesse o Caipora Sentinela",
  icons: {
    icon: "/logo-caipora.png",
    shortcut: "/logo-caipora.png",
    apple: "/logo-caipora.png",
  },
};

export default function CaiporaLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <>
      {/* O SDK aqui garante que o CSS esteja disponível para a rota /caipora */}
      <Script 
        src="https://sdk.mercadopago.com/js/v2" 
        strategy="beforeInteractive" 
      />
      {children}
    </>
  );
}
