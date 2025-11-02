import { Metadata } from 'next';
import { notFound } from 'next/navigation';
import { getAllPostSlugs, getPostData, getSortedPostsData } from '@/lib/posts';
import { PostContent } from '@/components/blog/PostContent';
import { findSeriesForPost } from '@/lib/postOrganization';

interface BlogPostParams {
  params: {
    slug: string;
  };
}

export async function generateMetadata(props: any): Promise<Metadata> {
  const resolvedProps = await props;
  const { params } = resolvedProps;
  const resolvedParams = await params;
  const { slug } = resolvedParams;
  
  try {
    const post = await getPostData(slug);
    return {
      title: `${post.title} | AI & Family`,
      description: post.excerpt,
      openGraph: {
        title: post.title,
        description: post.excerpt,
        type: 'article',
        ...(post.coverImage && { images: [post.coverImage] }),
      },
    };
  } catch (error) {
    console.error(`Error generating metadata for ${slug}:`, error);
    return {
      title: 'Blog Post Not Found | AI & Family',
      description: 'The requested blog post could not be found.',
    };
  }
}

export async function generateStaticParams() {
  const paths = getAllPostSlugs();
  return paths;
}

export default async function BlogPost(props: any) {
  const resolvedProps = await props;
  const { params } = resolvedProps;
  const resolvedParams = await params;
  const { slug } = resolvedParams;
  
  try {
    const post = await getPostData(slug);
    // Find if this post belongs to a series
    const allPosts = getSortedPostsData();
    const series = findSeriesForPost(slug, allPosts);
    
    return <PostContent post={post} series={series || undefined} />;
  } catch (error) {
    console.error(`Error loading post ${slug}:`, error);
    notFound();
  }
} 