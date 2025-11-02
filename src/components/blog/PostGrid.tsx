import { PostCard } from './PostCard';
import { SeriesCard } from './SeriesCard';
import { PostMeta } from '@/lib/posts';
import { organizePosts } from '@/lib/postOrganization';

interface PostGridProps {
  posts: PostMeta[];
}

export function PostGrid({ posts }: PostGridProps) {
  const { series, standalonePosts } = organizePosts(posts);

  return (
    <div className="container mx-auto px-4 py-12">
      <h2 className="text-3xl font-bold tracking-tight mb-8">Latest Articles</h2>
      
      {/* Series Section */}
      {series.length > 0 && (
        <div className="mb-12">
          <h3 className="text-xl font-semibold mb-6 text-muted-foreground">Featured Series</h3>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {series.map((s) => (
              <SeriesCard key={s.masterPost.slug} series={s} />
            ))}
          </div>
        </div>
      )}

      {/* Individual Articles Section */}
      {standalonePosts.length > 0 && (
        <div>
          <h3 className="text-xl font-semibold mb-6 text-muted-foreground">
            {series.length > 0 ? 'Individual Articles' : ''}
          </h3>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {standalonePosts.map((post) => (
              <PostCard key={post.slug} post={post} />
            ))}
          </div>
        </div>
      )}

      {/* Empty State */}
      {posts.length === 0 && (
        <div className="text-center p-12">
          <p className="text-muted-foreground">No blog posts found. Check back soon!</p>
        </div>
      )}
    </div>
  );
} 