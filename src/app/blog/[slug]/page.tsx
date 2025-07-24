import { Metadata } from 'next';
import { notFound } from 'next/navigation';
import { getAllPostSlugs, getPostData } from '@/lib/posts';
import { PostContent } from '@/components/blog/PostContent';

interface BlogPostParams {
  params: {
    slug: string;
  };
}

export async function generateMetadata(props: any): Promise<Metadata> {
  const { params } = await props;
  const { slug } = params;
  
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
  const { params } = await props;
  const { slug } = params;
  
  try {
    const post = await getPostData(slug);
    return <PostContent post={post} />;
  } catch (error) {
    console.error(`Error loading post ${slug}:`, error);
    notFound();
  }
} 