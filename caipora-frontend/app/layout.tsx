import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Agri-Market Intelligence | Risk Automation & Caipora Sentinela",
  description: "Líder em inteligência geoespacial e automação de riscos para o agronegócio global.",
  icons: {
    icon: "/logo-caipora.jpg", // Certifique-se de que o arquivo existe na pasta public
  },
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="pt-BR" className="scroll-smooth">
      <body>{children}</body>
    </html>
  );
}