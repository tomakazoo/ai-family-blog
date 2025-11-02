"use client"

import Link from 'next/link';
import Image from 'next/image';
import { CalendarIcon, TagIcon, BookOpen } from 'lucide-react';
import { Card, CardContent, CardFooter, CardHeader } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { PostMeta } from '@/lib/posts';
import { useState } from 'react';

interface PostCardProps {
  post: PostMeta;
  seriesParts?: number; // Number of parts in the series (only for master post)
  isSeriesPart?: boolean; // True if this is an individual part of a series
  partNumber?: number; // Part number (1-7) for series parts
}

export function PostCard({ post, seriesParts, isSeriesPart, partNumber }: PostCardProps) {
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
    <Card className={`h-full flex flex-col overflow-hidden hover:shadow-lg transition-shadow ${seriesParts ? 'border-2 border-primary/20' : ''} ${isSeriesPart ? 'border-l-4 border-l-primary/30' : ''}`}>
      <CardHeader className="p-0 relative">
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
          {seriesParts && (
            <div className="absolute top-2 right-2 bg-primary/90 text-primary-foreground px-3 py-1 rounded-full text-xs font-semibold flex items-center gap-1 shadow-lg">
              <BookOpen className="h-3 w-3" />
              <span>Series: {seriesParts} Parts</span>
            </div>
          )}
        </div>
      </CardHeader>
      <CardContent className="flex-grow p-4">
        <div className="space-y-2">
          <div className="flex items-start justify-between gap-2">
            <h3 className="text-xl font-bold tracking-tight flex-1">
              {isSeriesPart && partNumber && (
                <span className="inline-block mr-2 w-6 h-6 rounded-full bg-primary/10 text-primary text-xs font-bold flex items-center justify-center">
                  {partNumber.toString().padStart(2, '0')}
                </span>
              )}
              {title}
            </h3>
          </div>
          <div className="flex items-center text-sm text-muted-foreground">
            <CalendarIcon className="mr-1 h-3 w-3" />
            <time dateTime={date}>{formattedDate}</time>
            {seriesParts && (
              <>
                <span className="mx-2">•</span>
                <span className="flex items-center gap-1 text-primary font-medium">
                  <BookOpen className="h-3 w-3" />
                  {seriesParts}-Part Series
                </span>
              </>
            )}
            {isSeriesPart && (
              <>
                <span className="mx-2">•</span>
                <span className="text-primary font-medium">Series Part</span>
              </>
            )}
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