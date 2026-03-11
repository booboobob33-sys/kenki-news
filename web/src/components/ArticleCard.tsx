import Link from 'next/link';
import type { Article } from '@/types/article';

function formatDate(dateStr: string | null): string {
  if (!dateStr) return '';
  return new Date(dateStr).toLocaleDateString('ja-JP', {
    year: 'numeric',
    month: 'long',
    day: 'numeric',
  });
}

function Tag({ label }: { label: string }) {
  return (
    <span className="inline-block bg-blue-100 text-blue-800 text-xs font-medium px-2 py-0.5 rounded">
      {label}
    </span>
  );
}

export default function ArticleCard({ article }: { article: Article }) {
  const displayTitle = article.titleJP || article.titleEN;
  const subTitle     = article.titleJP ? article.titleEN : '';

  return (
    <Link href={`/articles/${article.id}`} className="block group">
      <article className="bg-white rounded-xl shadow-sm border border-gray-100 p-5 hover:shadow-md hover:border-blue-200 transition-all duration-200">
        {/* メタ情報 */}
        <div className="flex items-center gap-2 text-xs text-gray-400 mb-2">
          {article.publishedDate && (
            <span>{formatDate(article.publishedDate)}</span>
          )}
          {article.source && (
            <>
              <span>·</span>
              <span className="font-medium text-gray-500">{article.source}</span>
            </>
          )}
        </div>

        {/* タイトル */}
        <h2 className="text-base font-bold text-gray-900 leading-snug group-hover:text-blue-700 transition-colors line-clamp-2 mb-1">
          {displayTitle}
        </h2>
        {subTitle && (
          <p className="text-xs text-gray-400 mb-2 line-clamp-1">{subTitle}</p>
        )}

        {/* タグ */}
        {(article.brand.length > 0 || article.segment.length > 0 || article.region.length > 0) && (
          <div className="flex flex-wrap gap-1 mt-3">
            {article.brand.map(b   => <Tag key={b} label={b} />)}
            {article.segment.map(s => <Tag key={s} label={s} />)}
            {article.region.map(r  => <Tag key={r} label={r} />)}
          </div>
        )}
      </article>
    </Link>
  );
}
