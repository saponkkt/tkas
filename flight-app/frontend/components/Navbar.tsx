'use client';

import Link from 'next/link';

export default function Navbar() {
  return (
    <nav className="fixed top-0 left-0 right-0 z-50 bg-white border-b border-gray-100 shadow-sm h-16">
      <div className="max-w-7xl mx-auto px-6 h-full flex items-center">
        <Link href="/" className="flex items-center gap-2.5">
          <svg
            className="w-7 h-7 text-blue-600"
            fill="none"
            stroke="currentColor"
            viewBox="0 0 24 24"
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={2}
              d="M12 19l9 2-9-18-9 18 9-2zm0 0v-8"
            />
          </svg>
          <span className="text-2xl font-bold text-blue-600 tracking-tight">
            TKAS
          </span>
        </Link>
      </div>
    </nav>
  );
}
