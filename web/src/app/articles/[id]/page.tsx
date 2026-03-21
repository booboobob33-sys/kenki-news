import { notFound } from 'next/navigation';
import Link from 'next/link';
import { getArticleById, getArticleBlocks, getArticles } from '@/lib/notion';
import type { ContentBlock } from '@/types/article';

export const revalidate = 1800;

export async function generateStaticParams() {
  const articles = await getArticles();
  return articles.map(a => ({ id: a.id }));
}

function formatDate(dateStr: string | null): string {
  if (!dateStr) return '';
  return new Date(dateStr).toLocaleDateString('ja-JP', {
    year: 'numeric', month: 'long', day: 'numeric',
  });
}

function Tag({ label }: { label: string }) {
  return (
    <span className="inline-block bg-blue-100 text-blue-800 text-xs font-medium px-2 py-0.5 rounded">
      {label}
    </span>
  );
}

function BlockRenderer({ block }: { block: ContentBlock }) {
  switch (block.type) {
    case 'heading':
      return <h2 className="text-lg font-bold text-gray-800 mt-8 mb-3 border-b pb-1">{block.content}</h2>;
    case 'bullet':
      return (
        <li className="text-gray-700 leading-relaxed ml-4 list-disc">{block.content}</li>
      );
    case 'link':
      return (
        <p className="text-sm text-gray-400 mt-4">
          {block.url ? (
            <a href={block.url} target="_blank" rel="noopener noreferrer"
               className="text-blue-600 hover:underline break-all">
              {block.content}
            </a>
          ) : block.content}
        </p>
      );
    default:
      return <p className="text-gray-700 leading-relaxed">{block.content}</p>;
  }
}

export default async function ArticlePage({ params }: { params: { id: string } }) {
  const [article, blocks] = await Promise.all([
    getArticleById(params.id),
    getArticleBlocks(params.id),
  ]);

  if (!article) notFound();

  const allTags = [...article.brand, ...article.segment, ...article.region];

  return (
    <div className="max-w-3xl mx-auto">
      {/* パンくず */}
      <Link href="/" className="text-sm text-blue-600 hover:underline mb-6 inline-block">
        ← 一覧に戻る
      </Link>

      {/* ヘッダー */}
      <header className="mb-8">
        <div className="flex items-center gap-2 text-xs text-gray-400 mb-3">
          {article.publishedDate && <span>{formatDate(article.publishedDate)}</span>}
          {article.source && (
            <>
              <span>·</span>
              <span className="font-medium text-gray-500">{article.source}</span>
            </>
          )}
        </div>

        <h1 className="text-2xl font-bold text-gray-900 leading-tight mb-2">
          {article.titleJP || article.titleEN}
        </h1>
        {article.titleJP && article.titleEN && (
          <p className="text-sm text-gray-400">{article.titleEN}</p>
        )}

        {allTags.length > 0 && (
          <div className="flex flex-wrap gap-1.5 mt-4">
            {allTags.map(t => <Tag key={t} label={t} />)}
          </div>
        )}

        {article.sourceUrl && (
          <a href={article.sourceUrl} target="_blank" rel="noopener noreferrer"
             className="inline-block mt-4 text-xs text-blue-600 hover:underline break-all">
            🔗 元記事を開く
          </a>
        )}
      </header>

      {/* 本文ブロック */}
      <div className="bg-white rounded-xl border border-gray-100 shadow-sm p-6 space-y-2">
        {blocks.length === 0 ? (
          <p className="text-gray-400">本文がありません。</p>
        ) : (
          blocks.map((block, i) => <BlockRenderer key={i} block={block} />)
        )}
      </div>

      {/* 著作権表示 */}
      <div className="mt-6 p-4 bg-yellow-50 border border-yellow-200 rounded-lg text-xs text-yellow-800 space-y-1">
        <p>
          <span className="font-bold">⚠️ 著作権表示：</span>
          本ページは著作権法第32条に基づく引用および社内情報収集・研究目的で転記・翻訳しています。
          著作権は原著作者に帰属します。商用利用・外部公開を禁じます。
        </p>
        {article.sourceUrl && (
          <p>
            原文：
            <a
              href={article.sourceUrl}
              target="_blank"
              rel="noopener noreferrer"
              className="underline break-all"
            >
              {article.sourceUrl}
            </a>
          </p>
        )}
      </div>
    </div>
  );
}
