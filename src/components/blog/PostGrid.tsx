import { PostCard } from './PostCard';
import { PostMeta } from '@/lib/posts';

interface PostGridProps {
  posts: PostMeta[];
}

export function PostGrid({ posts }: PostGridProps) {
  return (
    <div className="container mx-auto px-4 py-12">
      <h2 className="text-3xl font-bold tracking-tight mb-8">Latest Articles</h2>
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        {posts.map((post) => (
          <PostCard key={post.slug} post={post} />
        ))}
      </div>
      {posts.length === 0 && (
        <div className="text-center p-12">
          <p className="text-muted-foreground">No blog posts found. Check back soon!</p>
        </div>
      )}
    </div>
  );
} 