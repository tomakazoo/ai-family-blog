"use client"

import Image from 'next/image';
import { Post } from '@/lib/posts';
import { PostSeries } from '@/lib/postOrganization';
import { SeriesNavigation } from './SeriesNavigation';
import '@/styles/markdown.css';
import { useState, useEffect, useRef } from 'react';
import mermaid from 'mermaid';

// Initialize mermaid ONCE
let mermaidInitialized = false;

const initializeMermaid = () => {
  if (!mermaidInitialized) {
    mermaid.initialize({
      startOnLoad: false,
      theme: 'default',
      securityLevel: 'loose',
    });
    mermaidInitialized = true;
  }
};

interface PostContentProps {
  post: Post;
  series?: PostSeries;
}

export function PostContent({ post, series }: PostContentProps) {
  const { content, coverImage, slug } = post;
  const [imgError, setImgError] = useState(false);
  const contentRef = useRef<HTMLDivElement>(null);

  const handleImageError = () => {
    setImgError(true);
  };

  const imageSrc = imgError ? '/images/blog/default.jpg' : (coverImage || '/images/blog/default.jpg');

  useEffect(() => {
    initializeMermaid();

    const processMermaidDiagrams = async () => {
      if (!contentRef.current) return;

      const preElements = contentRef.current.querySelectorAll('pre code.language-mermaid');
      
      if (preElements.length > 0) {
        for (const preCode of preElements) {
          const pre = preCode.parentElement as HTMLElement;
          if (!pre || pre.getAttribute('data-mermaid-processed') === 'true') continue;

          let mermaidCode = preCode.textContent?.trim() || '';
          if (!mermaidCode) continue;

          try {
            const id = `mermaid-${Date.now()}-${Math.random().toString(36).substring(2, 11)}`;
            const { svg } = await mermaid.render(id, mermaidCode);
            
            // Create replacement div
            const wrapper = document.createElement('div');
            wrapper.className = 'mermaid-diagram';
            wrapper.innerHTML = svg;
            
            // Replace the pre element with the rendered diagram
            pre.parentNode?.replaceChild(wrapper, pre);
            wrapper.setAttribute('data-mermaid-processed', 'true');
          } catch (error) {
            console.error('Mermaid rendering error:', error);
            // Keep the original code block if rendering fails
            pre.setAttribute('data-mermaid-processed', 'true');
          }
        }
      }
    };

    // Small delay to ensure DOM is ready
    const timer = setTimeout(() => {
      processMermaidDiagrams();
    }, 100);

    return () => clearTimeout(timer);
  }, [content]);

  return (
    <div className="min-h-screen bg-white dark:bg-[#1e1e1e] py-8">
      <div className="container mx-auto px-4 max-w-4xl">
        <div className="mb-8 -mx-4">
          <div className="relative w-full h-64 md:h-96 overflow-hidden">
            <Image 
              src={imageSrc}
              alt={post.title}
              fill
              sizes="(max-width: 768px) 100vw, (max-width: 1200px) 100vw, 896px"
              className="object-cover"
              priority
              onError={handleImageError}
            />
          </div>
        </div>

        {/* Series Navigation - shown if post belongs to a series */}
        {series && (
          <SeriesNavigation series={series} currentSlug={slug} />
        )}

        <div 
          ref={contentRef}
          className="markdown-body"
          dangerouslySetInnerHTML={{ __html: content }} 
        />
      </div>
    </div>
  );
}
