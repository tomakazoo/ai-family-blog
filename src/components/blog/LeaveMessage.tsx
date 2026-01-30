'use client';

import { useState, useEffect } from 'react';
import { Button } from '@/components/ui/button';
import { Mail } from 'lucide-react';

const FORMSPREE_ENDPOINT = process.env.NEXT_PUBLIC_FORMSPREE_ENDPOINT ?? '';
const CONTACT_EMAIL = process.env.NEXT_PUBLIC_CONTACT_EMAIL ?? '';

export interface LeaveMessageProps {
  /** When on a blog post, pass the post title for subject/context */
  postTitle?: string;
  /** When on a blog post, pass the slug to build the post URL in the message */
  postSlug?: string;
  /** Optional: compact layout for footer */
  variant?: 'default' | 'compact';
}

export function LeaveMessage({
  postTitle,
  postSlug,
  variant = 'default',
}: LeaveMessageProps) {
  const [postUrl, setPostUrl] = useState<string>('');
  const [submitted, setSubmitted] = useState(false);
  const [submitting, setSubmitting] = useState(false);

  useEffect(() => {
    if (typeof window !== 'undefined' && postSlug) {
      setPostUrl(`${window.location.origin}/blog/${postSlug}`);
    }
  }, [postSlug]);

  const subject = postTitle
    ? `Message from blog – ${postTitle}`
    : 'Message from blog';
  const body = postUrl
    ? `\n\n---\nRegarding: ${postTitle ?? 'Blog'}\n${postUrl}`
    : '';

  const mailtoHref = CONTACT_EMAIL
    ? `mailto:${CONTACT_EMAIL}?subject=${encodeURIComponent(subject)}&body=${encodeURIComponent(body)}`
    : `mailto:?subject=${encodeURIComponent(subject)}&body=${encodeURIComponent(body)}`;

  const useFormspree = Boolean(FORMSPREE_ENDPOINT);
  const useMailto = Boolean(CONTACT_EMAIL) || !useFormspree;

  const handleSubmit = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    if (!FORMSPREE_ENDPOINT) return;
    const form = e.currentTarget;
    const formData = new FormData(form);
    if (postSlug && typeof window !== 'undefined') {
      formData.append('_post_url', `${window.location.origin}/blog/${postSlug}`);
    }
    setSubmitting(true);
    try {
      const res = await fetch(FORMSPREE_ENDPOINT, {
        method: 'POST',
        body: formData,
        headers: { Accept: 'application/json' },
      });
      if (res.ok) {
        setSubmitted(true);
        form.reset();
      }
    } finally {
      setSubmitting(false);
    }
  };

  if (!useFormspree && !useMailto) {
    return (
      <section
        className="rounded-lg border border-border bg-muted/30 p-4 text-sm text-muted-foreground"
        aria-label="Leave a message"
      >
        <p>
          To enable the contact form, set{' '}
          <code className="rounded bg-muted px-1">NEXT_PUBLIC_FORMSPREE_ENDPOINT</code> or{' '}
          <code className="rounded bg-muted px-1">NEXT_PUBLIC_CONTACT_EMAIL</code> in your
          environment.
        </p>
      </section>
    );
  }

  if (submitted) {
    return (
      <section
        className="rounded-lg border border-border bg-muted/30 p-4"
        aria-label="Leave a message"
      >
        <p className="text-sm text-muted-foreground">
          Thanks — your message was sent. I&apos;ll get back to you when I can.
        </p>
      </section>
    );
  }

  if (useFormspree) {
    return (
      <section
        className={variant === 'compact' ? 'rounded-lg border border-border bg-muted/30 p-4' : 'mt-10 rounded-lg border border-border bg-muted/30 p-6'}
        aria-label="Leave a message"
      >
        <h2 className="text-lg font-semibold mb-3">
          {postTitle ? 'Questions about this post?' : 'Leave a message'}
        </h2>
        <p className="text-sm text-muted-foreground mb-4">
          {postTitle
            ? 'Send a message and I\'ll get back to you.'
            : 'Have a question or feedback? Send me a message.'}
        </p>
        <form onSubmit={handleSubmit} className="space-y-4">
          <input
            type="hidden"
            name="_subject"
            value={subject}
          />
          {postTitle && (
            <input type="hidden" name="_post_title" value={postTitle} />
          )}
          <div className="grid gap-2">
            <label htmlFor="leave-message-name" className="text-sm font-medium">
              Name
            </label>
            <input
              id="leave-message-name"
              name="name"
              type="text"
              required
              className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
              placeholder="Your name"
            />
          </div>
          <div className="grid gap-2">
            <label htmlFor="leave-message-email" className="text-sm font-medium">
              Email
            </label>
            <input
              id="leave-message-email"
              name="email"
              type="email"
              required
              className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
              placeholder="your@email.com"
            />
          </div>
          <div className="grid gap-2">
            <label htmlFor="leave-message-message" className="text-sm font-medium">
              Message
            </label>
            <textarea
              id="leave-message-message"
              name="message"
              required
              rows={variant === 'compact' ? 3 : 4}
              className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring resize-y"
              placeholder="Your message..."
            />
          </div>
          <div className="flex flex-wrap items-center gap-3">
            <Button type="submit" disabled={submitting}>
              {submitting ? 'Sending...' : 'Send message'}
            </Button>
            {useMailto && CONTACT_EMAIL && (
              <a
                href={mailtoHref}
                className="text-sm text-muted-foreground hover:text-foreground inline-flex items-center gap-1.5"
              >
                <Mail className="h-4 w-4" />
                Or email directly
              </a>
            )}
          </div>
        </form>
      </section>
    );
  }

  return (
    <section
      className={variant === 'compact' ? 'rounded-lg border border-border bg-muted/30 p-4' : 'mt-10 rounded-lg border border-border bg-muted/30 p-6'}
      aria-label="Leave a message"
    >
      <h2 className="text-lg font-semibold mb-3">
        {postTitle ? 'Questions about this post?' : 'Leave a message'}
      </h2>
      <p className="text-sm text-muted-foreground mb-4">
        {postTitle
          ? 'Send a message and I\'ll get back to you.'
          : 'Have a question or feedback? Send me a message.'}
      </p>
      <Button asChild variant="default">
        <a href={mailtoHref} className="inline-flex items-center gap-2">
          <Mail className="h-4 w-4" />
          Email me
        </a>
      </Button>
    </section>
  );
}
