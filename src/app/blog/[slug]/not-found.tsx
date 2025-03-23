import Link from 'next/link';

export default function NotFound() {
  return (
    <div className="container mx-auto px-4 py-16 text-center">
      <h2 className="text-3xl font-bold mb-4">Blog Post Not Found</h2>
      <p className="text-muted-foreground mb-8">The blog post you're looking for doesn't exist or has been removed.</p>
      <Link 
        href="/" 
        className="px-4 py-2 rounded-md bg-primary text-primary-foreground hover:bg-primary/90 transition-colors"
      >
        Back to Home
      </Link>
    </div>
  );
} 