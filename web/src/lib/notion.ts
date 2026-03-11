import { Client } from '@notionhq/client';
import type { Article, ContentBlock } from '@/types/article';

const notion = new Client({ auth: process.env.NOTION_TOKEN });
const DATABASE_ID = process.env.NOTION_DATABASE_ID!;

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const getText    = (prop: any): string => prop?.rich_text?.[0]?.plain_text ?? prop?.title?.[0]?.plain_text ?? '';
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const getSelect  = (prop: any): string => prop?.select?.name ?? '';
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const getMulti   = (prop: any): string[] => prop?.multi_select?.map((s: any) => s.name) ?? [];
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const getDate    = (prop: any): string | null => prop?.date?.start ?? null;
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const getUrl     = (prop: any): string => prop?.url ?? '';

export async function getArticles(): Promise<Article[]> {
  const results: Article[] = [];
  let cursor: string | undefined = undefined;

  do {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const res: any = await notion.databases.query({
      database_id: DATABASE_ID,
      sorts: [{ property: 'Published Date（記事日付）', direction: 'descending' }],
      page_size: 100,
      ...(cursor ? { start_cursor: cursor } : {}),
    });

    for (const page of res.results) {
      if (!('properties' in page)) continue;
      const p = page.properties;
      results.push({
        id:            page.id,
        titleEN:       getText(p['Title(EN)']),
        titleJP:       getText(p['Title(JP)']),
        source:        getSelect(p['Source']),
        sourceUrl:     getUrl(p['Source URL']),
        publishedDate: getDate(p['Published Date（記事日付）']),
        brand:         getMulti(p['Brand']),
        region:        getMulti(p['Region']),
        segment:       getMulti(p['Segment']),
      });
    }

    cursor = res.has_more ? res.next_cursor : undefined;
  } while (cursor && results.length < 200);

  return results;
}

export async function getArticleById(id: string): Promise<Article | null> {
  try {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const page: any = await notion.pages.retrieve({ page_id: id });
    const p = page.properties;
    return {
      id:            page.id,
      titleEN:       getText(p['Title(EN)']),
      titleJP:       getText(p['Title(JP)']),
      source:        getSelect(p['Source']),
      sourceUrl:     getUrl(p['Source URL']),
      publishedDate: getDate(p['Published Date（記事日付）']),
      brand:         getMulti(p['Brand']),
      region:        getMulti(p['Region']),
      segment:       getMulti(p['Segment']),
    };
  } catch {
    return null;
  }
}

export async function getArticleBlocks(pageId: string): Promise<ContentBlock[]> {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const res: any = await notion.blocks.children.list({ block_id: pageId });
  const blocks: ContentBlock[] = [];

  for (const block of res.results) {
    switch (block.type) {
      case 'heading_2':
        blocks.push({
          type: 'heading',
          content: block.heading_2.rich_text[0]?.plain_text ?? '',
        });
        break;
      case 'paragraph': {
        // リンク付きテキストを検出
        const richText = block.paragraph.rich_text ?? [];
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const text = richText.map((r: any) => r.plain_text).join('');
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const link = richText.find((r: any) => r.href)?.href;
        if (text) {
          blocks.push({ type: link ? 'link' : 'paragraph', content: text, url: link });
        }
        break;
      }
      case 'bulleted_list_item':
        blocks.push({
          type: 'bullet',
          content: block.bulleted_list_item.rich_text[0]?.plain_text ?? '',
        });
        break;
    }
  }

  return blocks.filter(b => b.content);
}
