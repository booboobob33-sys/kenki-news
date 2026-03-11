import { getArticles } from '@/lib/notion';
import ArticleCard from '@/components/ArticleCard';

export const revalidate = 1800; // 30分ごとに再生成

export default async function HomePage() {
  const articles = await getArticles();

  return (
    <div>
      <div className="mb-8">
        <h1 className="text-2xl font-bold text-gray-800">最新ニュース</h1>
        <p className="text-sm text-gray-400 mt-1">{articles.length} 件の記事</p>
      </div>

      {articles.length === 0 ? (
        <p className="text-gray-400 text-center py-20">記事がありません。</p>
      ) : (
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-5">
          {articles.map(article => (
            <ArticleCard key={article.id} article={article} />
          ))}
        </div>
      )}
    </div>
  );
}
