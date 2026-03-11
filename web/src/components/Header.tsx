import Link from 'next/link';

export default function Header() {
  return (
    <header className="bg-brand-900 text-white shadow-md">
      <div className="max-w-6xl mx-auto px-4 py-4 flex items-center justify-between">
        <Link href="/" className="flex items-center gap-2 hover:opacity-80 transition-opacity">
          <span className="text-2xl">🏗️</span>
          <div>
            <p className="text-lg font-bold leading-tight">建機ニュース</p>
            <p className="text-xs text-blue-200">Construction Machinery News</p>
          </div>
        </Link>
        <p className="text-sm text-blue-200 hidden sm:block">
          建設・鉱山機械業界の最新情報
        </p>
      </div>
    </header>
  );
}
