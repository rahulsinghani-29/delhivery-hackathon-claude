const defaultTheme = require('tailwindcss/defaultTheme')

module.exports = {
  content: ['./index.html', './src/**/*.{js,ts,jsx,tsx}'],
  theme: {
    extend: {
      colors: {
        delhivery: {
          red: '#EE3C26',
          'red-dark': '#C42E1C',
          'red-light': '#FFF0EE',
        },
        success: { DEFAULT: '#1B8A4E', light: '#E8F5EE' },
        warning: { DEFAULT: '#D4850A', light: '#FFF8EC' },
        danger: { DEFAULT: '#EE3C26', light: '#FFF0EE' },
        info: { DEFAULT: '#2563EB', light: '#EFF6FF' },
        gray: {
          50: '#FAFAFA',
          100: '#F5F5F5',
          300: '#D1D1D1',
          500: '#8A8A8A',
          700: '#4A4A4A',
          900: '#1A1A1A',
        },
      },
      fontFamily: {
        sans: ['Inter', ...defaultTheme.fontFamily.sans],
        mono: ['JetBrains Mono', ...defaultTheme.fontFamily.mono],
      },
    },
  },
  plugins: [],
}
