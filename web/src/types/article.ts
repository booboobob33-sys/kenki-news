export type Article = {
  id: string;
  titleEN: string;
  titleJP: string;
  source: string;
  sourceUrl: string;
  publishedDate: string | null;
  brand: string[];
  region: string[];
  segment: string[];
};

export type ContentBlock = {
  type: 'heading' | 'paragraph' | 'bullet' | 'link';
  content: string;
  url?: string;
};
