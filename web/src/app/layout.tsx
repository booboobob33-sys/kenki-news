import type { Metadata } from 'next';
import { Inter } from 'next/font/google';
import './globals.css';
import Header from '@/components/Header';

const inter = Inter({ subsets: ['latin'] });

export const metadata: Metadata = {
  title: '建機ニュース | Construction Machinery News',
  description: '建設・鉱山機械業界の最新ニュースをお届けします。',
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="ja">
      <body className={`${inter.className} bg-gray-50 text-gray-900 min-h-screen`}>
        <Header />
        <main className="max-w-6xl mx-auto px-4 py-8">{children}</main>
        <footer className="border-t mt-16 py-6 text-center text-sm text-gray-400">
          © {new Date().getFullYear()} 建機ニュース. Powered by Notion & Next.js.
        </footer>
      </body>
    </html>
  );
}
