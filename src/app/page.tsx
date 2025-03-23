import { Hero } from '@/components/blog/Hero';
import { PostGrid } from '@/components/blog/PostGrid';
import { getSortedPostsData } from '@/lib/posts';

export default function Home() {
  const posts = getSortedPostsData();

  return (
    <main>
      <Hero />
      <PostGrid posts={posts} />
    </main>
  );
}
