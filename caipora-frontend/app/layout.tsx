import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Agri-Market Intelligence | Risk Automation & Caipora Sentinela",
  description: "Dados que plantam, tecnologia que protege.",
  icons: {
    icon: "/logo-agrimarket.png",
    shortcut: "/logo-agrimarket.png", 
    apple: "/logo-agrimarket.png",    
  },
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    // Adicionado suppressHydrationWarning aqui no html
    <html lang="pt-BR" className="scroll-smooth" suppressHydrationWarning>
      {/* Adicionado suppressHydrationWarning aqui no body */}
      <body suppressHydrationWarning>
        {children}
      </body>
    </html>
  );
}