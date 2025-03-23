import { BookOpen, Zap, Users } from 'lucide-react';

interface HeroProps {
  title?: string;
  subtitle?: string;
}

export function Hero({ 
  title = "AI & Family: A Game Plan for Life and Teamwork", 
  subtitle = "Explore how artificial intelligence can enhance family dynamics, productivity, and teamwork in everyday life." 
}: HeroProps) {
  return (
    <section className="relative py-20 overflow-hidden bg-gradient-to-b from-primary/10 to-background">
      <div className="container px-4 mx-auto relative z-10">
        <div className="max-w-4xl mx-auto text-center">
          <h1 className="text-4xl md:text-5xl lg:text-6xl font-bold tracking-tight mb-6">
            {title}
          </h1>
          <p className="text-xl text-muted-foreground max-w-2xl mx-auto mb-12">
            {subtitle}
          </p>
          
          <div className="grid grid-cols-1 md:grid-cols-3 gap-8 mt-16">
            <div className="flex flex-col items-center">
              <div className="h-12 w-12 rounded-full bg-primary/20 flex items-center justify-center mb-4">
                <BookOpen className="h-6 w-6 text-primary" />
              </div>
              <h3 className="text-lg font-medium mb-2">Insightful Content</h3>
              <p className="text-muted-foreground text-center">Thoughtful articles exploring the intersection of AI and family life.</p>
            </div>
            
            <div className="flex flex-col items-center">
              <div className="h-12 w-12 rounded-full bg-primary/20 flex items-center justify-center mb-4">
                <Zap className="h-6 w-6 text-primary" />
              </div>
              <h3 className="text-lg font-medium mb-2">Practical Tips</h3>
              <p className="text-muted-foreground text-center">Actionable strategies for implementing AI tools in your family routine.</p>
            </div>
            
            <div className="flex flex-col items-center">
              <div className="h-12 w-12 rounded-full bg-primary/20 flex items-center justify-center mb-4">
                <Users className="h-6 w-6 text-primary" />
              </div>
              <h3 className="text-lg font-medium mb-2">Family-Focused</h3>
              <p className="text-muted-foreground text-center">Centered on strengthening family bonds through thoughtful technology use.</p>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
} 