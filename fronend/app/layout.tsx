import type { Metadata } from "next";
import type { ReactNode } from "react";
import "@/styles/globals.css";
import { APP_NAME } from "@/lib/constants";
import { AppProviders } from "@/app/providers";

export const metadata: Metadata = {
  title: APP_NAME,
  description: "Portal frontend para control, observabilidad y operación ETL sobre FastAPI + Spark"
};

type Props = { children: ReactNode };

export default function RootLayout({ children }: Readonly<Props>) {
  return (
    <html lang="es">
      <body>
        <AppProviders>{children}</AppProviders>
      </body>
    </html>
  );
}
