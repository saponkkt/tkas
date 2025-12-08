import { useState, useEffect } from "react";

export default function Layout({ children }) {
  const [dark, setDark] = useState(true);

  useEffect(() => {
    if (dark) {
      document.documentElement.classList.add("dark");
    } else {
      document.documentElement.classList.remove("dark");
    }
  }, [dark]);

  return (
    <div className={dark ? "dark" : ""}>
      <div className="min-h-screen bg-slate-950 text-slate-100 dark:bg-slate-950">
        <header className="border-b border-slate-800 bg-slate-900/70 backdrop-blur">
          <div className="mx-auto flex max-w-5xl items-center justify-between px-4 py-3">
            <h1 className="text-lg font-semibold tracking-tight">
              Flight Analytics
            </h1>
            <button
              onClick={() => setDark((d) => !d)}
              className="rounded-full border border-slate-700 bg-slate-800 px-3 py-1 text-xs font-medium text-slate-100 shadow hover:bg-slate-700"
            >
              {dark ? "Light mode" : "Dark mode"}
            </button>
          </div>
        </header>
        <main className="mx-auto max-w-5xl px-4 py-6">{children}</main>
        <footer className="border-t border-slate-800 bg-slate-900/70 py-4 text-center text-xs text-slate-500">
          Built for FlightRadar24 CSV analysis demo
        </footer>
      </div>
    </div>
  );
}


