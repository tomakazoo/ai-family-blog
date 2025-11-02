import { PostMeta } from './posts';

export interface PostSeries {
  masterPost: PostMeta;
  parts: PostMeta[];
  seriesName: string;
}

/**
 * Detects if a post is part of a series
 */
function isSeriesPost(slug: string): boolean {
  return /^part-\d{2}-/.test(slug);
}

/**
 * Detects if a post is a master post for a series
 */
function isMasterPost(slug: string): boolean {
  return slug.includes('master-') || slug.includes('series');
}

/**
 * Extracts series name from post slugs or title
 */
function getSeriesName(masterPost: PostMeta): string {
  // Try to get a readable name from the title first
  if (masterPost.title && !masterPost.title.toLowerCase().includes('master')) {
    // Remove emojis, common prefixes/suffixes
    return masterPost.title
      .replace(/[\u{1F300}-\u{1F9FF}]/gu, '') // Remove emojis
      .replace(/^(Complete|The|A)\s+/i, '')
      .replace(/\s+(Series|Blog Series|Blog)$/i, '')
      .trim();
  }
  
  // Fallback to slug-based extraction
  const slug = masterPost.slug;
  return slug
    .replace(/^master-/, '')
    .replace(/-series$/, '')
    .replace(/-kafka$/, ' Kafka')
    .replace(/-event-driven-architecture/, ' Event-Driven Architecture')
    .replace(/-/g, ' ')
    .split(' ')
    .map(word => word.charAt(0).toUpperCase() + word.slice(1))
    .join(' ');
}

/**
 * Organizes posts into series and standalone posts
 */
export function organizePosts(posts: PostMeta[]): {
  series: PostSeries[];
  standalonePosts: PostMeta[];
} {
  const seriesMap = new Map<string, PostSeries>();
  const standalonePosts: PostMeta[] = [];
  const masterPosts: PostMeta[] = [];
  const partPosts: PostMeta[] = [];

  // First pass: categorize all posts
  posts.forEach(post => {
    if (isMasterPost(post.slug)) {
      masterPosts.push(post);
    } else if (isSeriesPost(post.slug)) {
      partPosts.push(post);
    } else {
      standalonePosts.push(post);
    }
  });

  // Match master posts with their parts
  // Only assign parts to the first master found (typically there's one master per series)
  if (masterPosts.length > 0 && partPosts.length > 0) {
    const master = masterPosts[0];
    const seriesName = getSeriesName(master);
    const parts = [...partPosts]; // Copy all parts
    
    // Clear partPosts since we're assigning them all to this master
    partPosts.length = 0;

    seriesMap.set(master.slug, {
      masterPost: master,
      parts: parts.sort((a, b) => {
        // Sort parts by their number (part-01, part-02, etc.)
        const aNum = parseInt(a.slug.match(/part-(\d{2})/)?.[1] || '0');
        const bNum = parseInt(b.slug.match(/part-(\d{2})/)?.[1] || '0');
        return aNum - bNum;
      }),
      seriesName,
    });

    // Any additional master posts become standalone
    for (let i = 1; i < masterPosts.length; i++) {
      standalonePosts.push(masterPosts[i]);
    }
  } else {
    // No matching master/parts, treat all as standalone
    masterPosts.forEach(master => standalonePosts.push(master));
  }

  // Any remaining parts without a master become standalone
  partPosts.forEach(part => standalonePosts.push(part));

  return {
    series: Array.from(seriesMap.values()),
    standalonePosts: standalonePosts.sort((a, b) => {
      // Sort standalone posts by date
      if (a.date < b.date) return 1;
      if (a.date > b.date) return -1;
      return 0;
    }),
  };
}

/**
 * Finds the series that a post belongs to (if any)
 */
export function findSeriesForPost(slug: string, allPosts: PostMeta[]): PostSeries | null {
  const { series } = organizePosts(allPosts);
  
  for (const s of series) {
    // Check if it's the master post
    if (s.masterPost.slug === slug) {
      return s;
    }
    // Check if it's one of the parts
    if (s.parts.some(part => part.slug === slug)) {
      return s;
    }
  }
  
  return null;
}
