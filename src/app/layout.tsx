import type { Metadata } from "next";
import { Inter } from "next/font/google";
import "./globals.css";
import Link from "next/link";
import { Github, Linkedin, Twitter } from "lucide-react";
import { LeaveMessage } from "@/components/blog/LeaveMessage";

const inter = Inter({ subsets: ["latin"] });

export const metadata: Metadata = {
  title: "AI & Family: A Game Plan for Life and Teamwork",
  description: "Explore how artificial intelligence can enhance family dynamics, productivity, and teamwork in everyday life.",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body className={inter.className}>
        <div className="flex min-h-screen flex-col">
          <header className="border-b bg-background">
            <div className="container mx-auto px-4 py-4 flex items-center justify-between">
              <Link href="/" className="text-xl font-bold">
                AI & Family
              </Link>
              <nav className="hidden md:flex items-center gap-6">
                <Link href="/" className="text-sm font-medium hover:text-primary transition-colors">
                  Home
                </Link>
                <Link href="/#blog" className="text-sm font-medium hover:text-primary transition-colors">
                  Blog
                </Link>
              </nav>
            </div>
          </header>
          <main className="flex-1">
            {children}
          </main>
          <footer className="border-t bg-muted/40">
            <div className="container mx-auto px-4 py-8">
              <div className="flex flex-col md:flex-row justify-between items-center gap-4">
                <div className="text-center md:text-left">
                  <h2 className="font-bold">AI & Family</h2>
                  <p className="text-sm text-muted-foreground">
                    A Game Plan for Life and Teamwork
                  </p>
                </div>
                <div className="flex items-center gap-4">
                  <a href="https://github.com" target="_blank" rel="noopener noreferrer" className="text-muted-foreground hover:text-foreground transition-colors">
                    <Github className="h-5 w-5" />
                    <span className="sr-only">GitHub</span>
                  </a>
                  <a href="https://twitter.com" target="_blank" rel="noopener noreferrer" className="text-muted-foreground hover:text-foreground transition-colors">
                    <Twitter className="h-5 w-5" />
                    <span className="sr-only">Twitter</span>
                  </a>
                  <a href="https://linkedin.com" target="_blank" rel="noopener noreferrer" className="text-muted-foreground hover:text-foreground transition-colors">
                    <Linkedin className="h-5 w-5" />
                    <span className="sr-only">LinkedIn</span>
                  </a>
                </div>
              </div>
              <div className="mt-8 max-w-2xl">
                <LeaveMessage variant="compact" />
              </div>
              <div className="mt-4 text-center md:text-left text-xs text-muted-foreground">
                &copy; {new Date().getFullYear()} AI & Family. All rights reserved.
              </div>
            </div>
          </footer>
        </div>
      </body>
    </html>
  );
}
