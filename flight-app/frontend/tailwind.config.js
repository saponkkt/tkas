/** @type {import('tailwindcss').Config} */
module.exports = {
  darkMode: "class",
  content: [
    "./pages/**/*.{js,ts,jsx,tsx}",
    "./app/**/*.{js,ts,jsx,tsx}",
    "./components/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      fontFamily: {
        sans: [
          'var(--font-inter)',
          'ui-sans-serif',
          'system-ui',
          'sans-serif',
        ],
      },
      keyframes: {
        fly: {
          '0%': { transform: 'translateX(-20px) translateY(0px)' },
          '25%': { transform: 'translateX(10px) translateY(-8px)' },
          '50%': { transform: 'translateX(40px) translateY(0px)' },
          '75%': { transform: 'translateX(70px) translateY(-8px)' },
          '100%': { transform: 'translateX(-20px) translateY(0px)' },
        },
      },
      animation: {
        fly: 'fly 3s ease-in-out infinite',
      },
    },
  },
  plugins: [],
};


