"use client"

import Image from 'next/image';
import { CalendarIcon, TagIcon, ClockIcon } from 'lucide-react';
import { Post } from '@/lib/posts';
import { useState } from 'react';

interface PostContentProps {
  post: Post;
}

export function PostContent({ post }: PostContentProps) {
  const { title, date, content, tags, coverImage } = post;
  const [imgError, setImgError] = useState(false);

  // Format the date
  const formattedDate = new Date(date).toLocaleDateString('en-US', {
    year: 'numeric',
    month: 'long',
    day: 'numeric',
  });

  // Estimate reading time (rough calculation: average adult reads ~200-250 words per minute)
  const wordCount = content.split(/\s+/).length;
  const readingTime = Math.max(1, Math.ceil(wordCount / 200));

  const handleImageError = () => {
    setImgError(true);
  };

  const imageSrc = imgError ? '/images/blog/default.jpg' : (coverImage || '/images/blog/default.jpg');

  return (
    <div className="bg-[#f7f7f7] min-h-screen py-10">
      <article className="bg-white rounded-2xl shadow-lg max-w-3xl mx-auto p-12">
        <header className="mb-8">
          <h1 className="text-3xl md:text-4xl font-bold tracking-tight mb-4">{title}</h1>
          {post.excerpt && (
            <p className="prose-lead mt-4 mb-8">{post.excerpt}</p>
          )}
          <div className="flex flex-wrap items-center gap-4 text-sm text-muted-foreground mb-6">
            <div className="flex items-center">
              <CalendarIcon className="mr-1 h-4 w-4" />
              <time dateTime={date}>{formattedDate}</time>
            </div>
            <div className="flex items-center">
              <ClockIcon className="mr-1 h-4 w-4" />
              <span>{readingTime} min read</span>
            </div>
          </div>
          <div className="relative w-full h-64 md:h-96 overflow-hidden rounded-lg mb-8">
            <Image 
              src={imageSrc}
              alt={title}
              fill
              sizes="(max-width: 768px) 100vw, (max-width: 1200px) 100vw, 75vw"
              className="object-cover"
              priority
              onError={handleImageError}
            />
          </div>
        </header>

        <div className="prose prose-lg max-w-none">
          <div dangerouslySetInnerHTML={{ __html: content }} />
        </div>

        {tags && tags.length > 0 && (
          <div className="mt-8 pt-6 border-t">
            <h2 className="text-lg font-semibold mb-3">Topics</h2>
            <div className="flex flex-wrap gap-2">
              {tags.map((tag) => (
                <span 
                  key={tag} 
                  className="inline-flex items-center px-3 py-1 text-sm font-medium rounded-full bg-primary/10 text-primary"
                >
                  <TagIcon className="mr-1 h-3 w-3" />
                  {tag}
                </span>
              ))}
            </div>
          </div>
        )}
      </article>
    </div>
  );
} 