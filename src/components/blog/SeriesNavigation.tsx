"use client"

import Link from 'next/link';
import { BookOpen, FileText, ChevronRight } from 'lucide-react';
import { PostSeries } from '@/lib/postOrganization';

interface SeriesNavigationProps {
  series: PostSeries;
  currentSlug?: string; // Current post slug to highlight active part
}

export function SeriesNavigation({ series, currentSlug }: SeriesNavigationProps) {
  const { masterPost, parts } = series;

  return (
    <div className="mb-8 p-6 bg-muted/50 rounded-lg border border-primary/20">
      <div className="flex items-center gap-2 mb-4">
        <BookOpen className="h-5 w-5 text-primary" />
        <h2 className="text-lg font-semibold">Series Navigation</h2>
        <span className="ml-auto text-sm text-muted-foreground">{parts.length} Parts</span>
      </div>
      
      {/* Master Post Link */}
      <Link
        href={`/blog/${masterPost.slug}`}
        className={`flex items-center gap-3 p-3 rounded-lg mb-3 transition-colors ${
          currentSlug === masterPost.slug
            ? 'bg-primary/10 border-2 border-primary/30'
            : 'border hover:bg-accent'
        }`}
      >
        <div className="flex-shrink-0 w-8 h-8 rounded-full bg-primary/20 text-primary flex items-center justify-center text-xs font-semibold">
          <BookOpen className="h-4 w-4" />
        </div>
        <div className="flex-1 min-w-0">
          <h3 className={`font-semibold ${currentSlug === masterPost.slug ? 'text-primary' : ''}`}>
            {masterPost.title}
          </h3>
          <p className="text-xs text-muted-foreground mt-1">Series Overview</p>
        </div>
        {currentSlug === masterPost.slug && (
          <span className="text-xs text-primary font-medium">Current</span>
        )}
      </Link>

      {/* Parts List */}
      <div className="space-y-2">
        <div className="flex items-center gap-2 mb-2">
          <FileText className="h-4 w-4 text-muted-foreground" />
          <span className="text-sm font-medium text-muted-foreground">Series Parts</span>
        </div>
        {parts.map((part, index) => {
          const partNumber = parseInt(part.slug.match(/part-(\d{2})/)?.[1] || String(index + 1).padStart(2, '0'));
          const isActive = currentSlug === part.slug;
          
          return (
            <Link
              key={part.slug}
              href={`/blog/${part.slug}`}
              className={`flex items-center gap-3 p-3 rounded-lg border transition-colors group ${
                isActive
                  ? 'bg-primary/10 border-2 border-primary/30'
                  : 'hover:bg-accent'
              }`}
            >
              <div className={`flex-shrink-0 w-8 h-8 rounded-full flex items-center justify-center text-xs font-semibold ${
                isActive
                  ? 'bg-primary text-primary-foreground'
                  : 'bg-primary/10 text-primary'
              }`}>
                {partNumber.toString().padStart(2, '0')}
              </div>
              <div className="flex-1 min-w-0">
                <h4 className={`text-sm font-medium line-clamp-1 ${
                  isActive ? 'text-primary' : 'group-hover:text-primary'
                } transition-colors`}>
                  {part.title}
                </h4>
                {part.excerpt && (
                  <p className="text-xs text-muted-foreground mt-1 line-clamp-1">
                    {part.excerpt}
                  </p>
                )}
              </div>
              <ChevronRight className={`h-4 w-4 flex-shrink-0 transition-colors ${
                isActive ? 'text-primary' : 'text-muted-foreground group-hover:text-primary'
              }`} />
            </Link>
          );
        })}
      </div>
    </div>
  );
}

