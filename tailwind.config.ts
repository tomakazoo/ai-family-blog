import type { Config } from "tailwindcss";

const config = {
  darkMode: "class",
  content: [
    './pages/**/*.{ts,tsx}',
    './components/**/*.{ts,tsx}',
    './app/**/*.{ts,tsx}',
    './src/**/*.{ts,tsx}',
  ],
  prefix: "",
  theme: {
    container: {
      center: true,
      padding: "2rem",
      screens: {
        "2xl": "1400px",
      },
    },
    extend: {
      colors: {
        border: "hsl(var(--border))",
        input: "hsl(var(--input))",
        ring: "hsl(var(--ring))",
        background: "hsl(var(--background))",
        foreground: "hsl(var(--foreground))",
        primary: {
          DEFAULT: "hsl(var(--primary))",
          foreground: "hsl(var(--primary-foreground))",
        },
        secondary: {
          DEFAULT: "hsl(var(--secondary))",
          foreground: "hsl(var(--secondary-foreground))",
        },
        destructive: {
          DEFAULT: "hsl(var(--destructive))",
          foreground: "hsl(var(--destructive-foreground))",
        },
        muted: {
          DEFAULT: "hsl(var(--muted))",
          foreground: "hsl(var(--muted-foreground))",
        },
        accent: {
          DEFAULT: "hsl(var(--accent))",
          foreground: "hsl(var(--accent-foreground))",
        },
        popover: {
          DEFAULT: "hsl(var(--popover))",
          foreground: "hsl(var(--popover-foreground))",
        },
        card: {
          DEFAULT: "hsl(var(--card))",
          foreground: "hsl(var(--card-foreground))",
        },
      },
      borderRadius: {
        lg: "var(--radius)",
        md: "calc(var(--radius) - 2px)",
        sm: "calc(var(--radius) - 4px)",
      },
      keyframes: {
        "accordion-down": {
          from: { height: "0" },
          to: { height: "var(--radix-accordion-content-height)" },
        },
        "accordion-up": {
          from: { height: "var(--radix-accordion-content-height)" },
          to: { height: "0" },
        },
      },
      animation: {
        "accordion-down": "accordion-down 0.2s ease-out",
        "accordion-up": "accordion-up 0.2s ease-out",
      },
      fontFamily: {
        sans: [
          'system-ui',
          'Segoe UI',
          'Arial',
          'Helvetica',
          'sans-serif',
        ],
        serif: [
          'Georgia',
          'Cambria',
          'Times New Roman',
          'Times',
          'serif',
        ],
      },
      typography: (theme) => ({
        DEFAULT: {
          css: {
            fontFamily: 'system-ui, Segoe UI, Arial, Helvetica, sans-serif',
            fontSize: '1.18rem',
            lineHeight: '1.9',
            color: '#222',
            fontWeight: '400',
            h1: {
              fontSize: '3rem',
              fontWeight: '900',
              marginTop: '3rem',
              marginBottom: '2.5rem',
              lineHeight: '1.15',
            },
            h2: {
              fontSize: '2.25rem',
              fontWeight: '800',
              marginTop: '2.5rem',
              marginBottom: '2rem',
              lineHeight: '1.2',
            },
            h3: {
              fontSize: '1.5rem',
              fontWeight: '700',
              marginTop: '2rem',
              marginBottom: '1.5rem',
              lineHeight: '1.25',
            },
            h4: {
              fontSize: '1.25rem',
              fontWeight: '700',
              marginTop: '1.5rem',
              marginBottom: '1.25rem',
              lineHeight: '1.3',
            },
            p: {
              marginTop: '1.7em',
              marginBottom: '1.7em',
              fontWeight: '400',
            },
            ul: {
              marginTop: '1.7em',
              marginBottom: '1.7em',
              paddingLeft: '1.5em',
            },
            ol: {
              marginTop: '1.7em',
              marginBottom: '1.7em',
              paddingLeft: '1.5em',
            },
            li: {
              marginTop: '1.2em',
              marginBottom: '1.2em',
              paddingLeft: '0.25em',
            },
            'li strong': {
              fontWeight: '700',
            },
            strong: {
              fontWeight: '700',
            },
            em: {
              fontStyle: 'italic',
            },
            '.lead': {
              fontSize: '1.25rem',
              color: '#444',
              marginBottom: '2rem',
            },
          },
        },
      }),
    },
  },
  plugins: [require("tailwindcss-animate"), require("@tailwindcss/typography")],
} satisfies Config;

export default config; 