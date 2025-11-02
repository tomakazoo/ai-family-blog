"use client"

import { useState } from 'react';
import Link from 'next/link';
import { ChevronDown, ChevronRight, BookOpen, FileText } from 'lucide-react';
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { PostSeries } from '@/lib/postOrganization';
import { cn } from '@/lib/utils';

interface SeriesFolderProps {
  series: PostSeries;
}

export function SeriesFolder({ series }: SeriesFolderProps) {
  const [isExpanded, setIsExpanded] = useState(false);
  const { masterPost, parts, seriesName } = series;

  // Format master post date
  const formattedDate = new Date(masterPost.date).toLocaleDateString('en-US', {
    year: 'numeric',
    month: 'long',
    day: 'numeric',
  });

  return (
    <Card className="overflow-hidden border-2 hover:border-primary/50 transition-all">
      <CardHeader 
        className="cursor-pointer bg-muted/30 hover:bg-muted/50 transition-colors"
        onClick={() => setIsExpanded(!isExpanded)}
      >
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3 flex-1">
            <div className="flex items-center gap-2">
              {isExpanded ? (
                <ChevronDown className="h-5 w-5 text-muted-foreground" />
              ) : (
                <ChevronRight className="h-5 w-5 text-muted-foreground" />
              )}
              <BookOpen className="h-5 w-5 text-primary" />
            </div>
            <div className="flex-1">
              <h3 className="text-lg font-semibold">{seriesName}</h3>
              <p className="text-sm text-muted-foreground mt-1">
                {parts.length} part series • {formattedDate}
              </p>
            </div>
          </div>
          <Link 
            href={`/blog/${masterPost.slug}`}
            onClick={(e) => e.stopPropagation()}
          >
            <Button variant="outline" size="sm" className="ml-2">
              View Master
            </Button>
          </Link>
        </div>
      </CardHeader>

      {isExpanded && (
        <CardContent className="pt-0">
          {/* Master Post Card */}
          <div className="mb-4">
            <div className="flex items-center gap-2 mb-2">
              <FileText className="h-4 w-4 text-primary" />
              <span className="text-sm font-medium text-muted-foreground">Master Post</span>
            </div>
            <Link 
              href={`/blog/${masterPost.slug}`}
              className="block p-3 rounded-lg bg-primary/5 hover:bg-primary/10 transition-colors border border-primary/20"
            >
              <h4 className="font-semibold">{masterPost.title}</h4>
              {masterPost.excerpt && (
                <p className="text-sm text-muted-foreground mt-1 line-clamp-2">
                  {masterPost.excerpt}
                </p>
              )}
            </Link>
          </div>

          {/* Series Parts */}
          <div>
            <div className="flex items-center gap-2 mb-3">
              <FileText className="h-4 w-4 text-muted-foreground" />
              <span className="text-sm font-medium text-muted-foreground">
                Series Parts ({parts.length})
              </span>
            </div>
            <div className="space-y-2">
              {parts.map((part, index) => {
                const partNumber = part.slug.match(/part-(\d{2})/)?.[1] || String(index + 1).padStart(2, '0');
                
                return (
                  <Link
                    key={part.slug}
                    href={`/blog/${part.slug}`}
                    className="flex items-start gap-3 p-3 rounded-lg border hover:bg-accent transition-colors group"
                  >
                    <div className="flex-shrink-0 w-8 h-8 rounded-full bg-primary/10 text-primary flex items-center justify-center text-xs font-semibold">
                      {partNumber}
                    </div>
                    <div className="flex-1 min-w-0">
                      <h4 className="font-medium group-hover:text-primary transition-colors">
                        {part.title}
                      </h4>
                      {part.excerpt && (
                        <p className="text-sm text-muted-foreground mt-1 line-clamp-1">
                          {part.excerpt}
                        </p>
                      )}
                    </div>
                    <ChevronRight className="h-4 w-4 text-muted-foreground group-hover:text-primary transition-colors flex-shrink-0" />
                  </Link>
                );
              })}
            </div>
          </div>
        </CardContent>
      )}
    </Card>
  );
}
