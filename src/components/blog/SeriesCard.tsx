"use client"

import { useState } from 'react';
import Link from 'next/link';
import Image from 'next/image';
import { CalendarIcon, BookOpen, ChevronDown, ChevronRight, FileText } from 'lucide-react';
import { Card, CardContent, CardFooter, CardHeader } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { PostSeries } from '@/lib/postOrganization';

interface SeriesCardProps {
  series: PostSeries;
}

export function SeriesCard({ series }: SeriesCardProps) {
  const { masterPost, parts } = series;
  const [isExpanded, setIsExpanded] = useState(false);
  const [imgError, setImgError] = useState(false);

  // Format the date
  const formattedDate = new Date(masterPost.date).toLocaleDateString('en-US', {
    year: 'numeric',
    month: 'long',
    day: 'numeric',
  });

  const handleImageError = () => {
    setImgError(true);
  };

  const imageSrc = imgError ? '/images/blog/default.jpg' : (masterPost.coverImage || '/images/blog/default.jpg');

  return (
    <Card className="h-full flex flex-col overflow-hidden hover:shadow-lg transition-shadow border-2 border-primary/20">
      <CardHeader className="p-0 relative">
        <div className="relative w-full h-48 overflow-hidden">
          <Image 
            src={imageSrc}
            alt={masterPost.title}
            fill
            sizes="(max-width: 768px) 100vw, (max-width: 1200px) 50vw, 33vw"
            className="object-cover"
            priority
            onError={handleImageError}
          />
          <div className="absolute top-2 right-2 bg-primary/90 text-primary-foreground px-3 py-1 rounded-full text-xs font-semibold flex items-center gap-1 shadow-lg">
            <BookOpen className="h-3 w-3" />
            <span>Series: {parts.length} Parts</span>
          </div>
        </div>
      </CardHeader>
      
      <CardContent className="flex-grow p-4">
        <div className="space-y-2">
          <h3 className="text-xl font-bold tracking-tight">{masterPost.title}</h3>
          <div className="flex items-center text-sm text-muted-foreground">
            <CalendarIcon className="mr-1 h-3 w-3" />
            <time dateTime={masterPost.date}>{formattedDate}</time>
            <span className="mx-2">•</span>
            <span className="flex items-center gap-1 text-primary font-medium">
              <BookOpen className="h-3 w-3" />
              {parts.length}-Part Series
            </span>
          </div>
          {masterPost.excerpt && (
            <p className="text-muted-foreground line-clamp-2">{masterPost.excerpt}</p>
          )}

          {/* Expandable Parts Section */}
          <div className="mt-4 pt-4 border-t">
            <button
              onClick={() => setIsExpanded(!isExpanded)}
              className="flex items-center justify-between w-full text-left hover:text-primary transition-colors"
            >
              <span className="text-sm font-semibold flex items-center gap-2">
                <FileText className="h-4 w-4" />
                Series Parts ({parts.length})
              </span>
              {isExpanded ? (
                <ChevronDown className="h-4 w-4" />
              ) : (
                <ChevronRight className="h-4 w-4" />
              )}
            </button>

            {isExpanded && (
              <div className="mt-3 space-y-2">
                {parts.map((part, index) => {
                  const partNumber = parseInt(part.slug.match(/part-(\d{2})/)?.[1] || String(index + 1).padStart(2, '0'));
                  return (
                    <Link
                      key={part.slug}
                      href={`/blog/${part.slug}`}
                      className="flex items-center gap-3 p-2 rounded-lg border hover:bg-accent transition-colors group"
                    >
                      <div className="flex-shrink-0 w-8 h-8 rounded-full bg-primary/10 text-primary flex items-center justify-center text-xs font-semibold">
                        {partNumber.toString().padStart(2, '0')}
                      </div>
                      <div className="flex-1 min-w-0">
                        <h4 className="text-sm font-medium group-hover:text-primary transition-colors line-clamp-1">
                          {part.title}
                        </h4>
                      </div>
                      <ChevronRight className="h-4 w-4 text-muted-foreground group-hover:text-primary transition-colors flex-shrink-0" />
                    </Link>
                  );
                })}
              </div>
            )}
          </div>
        </div>
      </CardContent>

      <CardFooter className="flex justify-between items-center p-4 pt-0">
        <div className="flex flex-wrap gap-1">
          {masterPost.tags && masterPost.tags.map((tag) => (
            <span key={tag} className="inline-flex items-center px-2 py-1 text-xs font-medium rounded-md bg-primary/10 text-primary">
              {tag}
            </span>
          ))}
        </div>
        <Link href={`/blog/${masterPost.slug}`} passHref>
          <Button variant="outline" size="sm">Read More</Button>
        </Link>
      </CardFooter>
    </Card>
  );
}

