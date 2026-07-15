import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Murmur — Agent Swarm Orchestrator",
  description:
    "Watch a swarm of AI agents self-organize to solve what one agent can't: plan, delegate, validate, and synthesize — live.",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" className="h-full antialiased">
      <body className="min-h-full flex flex-col">{children}</body>
    </html>
  );
}
