"use client"

import Link from 'next/link';
import Image from 'next/image';
import { CalendarIcon, TagIcon } from 'lucide-react';
import { Card, CardContent, CardFooter, CardHeader } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { PostMeta } from '@/lib/posts';
import { useState } from 'react';

interface PostCardProps {
  post: PostMeta;
}

export function PostCard({ post }: PostCardProps) {
  const { slug, title, date, excerpt, tags, coverImage } = post;
  const [imgError, setImgError] = useState(false);
  
  // Format the date
  const formattedDate = new Date(date).toLocaleDateString('en-US', {
    year: 'numeric',
    month: 'long',
    day: 'numeric',
  });

  const handleImageError = () => {
    setImgError(true);
  };

  const imageSrc = imgError ? '/images/blog/default.jpg' : (coverImage || '/images/blog/default.jpg');

  return (
    <Card className="h-full flex flex-col overflow-hidden hover:shadow-lg transition-shadow">
      <CardHeader className="p-0">
        <div className="relative w-full h-48 overflow-hidden">
          <Image 
            src={imageSrc}
            alt={title}
            fill
            sizes="(max-width: 768px) 100vw, (max-width: 1200px) 50vw, 33vw"
            className="object-cover"
            priority
            onError={handleImageError}
          />
        </div>
      </CardHeader>
      <CardContent className="flex-grow p-4">
        <div className="space-y-2">
          <h3 className="text-xl font-bold tracking-tight">{title}</h3>
          <div className="flex items-center text-sm text-muted-foreground">
            <CalendarIcon className="mr-1 h-3 w-3" />
            <time dateTime={date}>{formattedDate}</time>
          </div>
          <p className="text-muted-foreground line-clamp-3">{excerpt}</p>
        </div>
      </CardContent>
      <CardFooter className="flex justify-between items-center p-4 pt-0">
        <div className="flex flex-wrap gap-1">
          {tags && tags.map((tag) => (
            <span key={tag} className="inline-flex items-center px-2 py-1 text-xs font-medium rounded-md bg-primary/10 text-primary">
              <TagIcon className="mr-1 h-3 w-3" />
              {tag}
            </span>
          ))}
        </div>
        <Link href={`/blog/${slug}`} passHref>
          <Button variant="outline" size="sm">Read More</Button>
        </Link>
      </CardFooter>
    </Card>
  );
} 