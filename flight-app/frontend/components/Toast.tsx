'use client';

interface ToastProps {
  message: string;
  type?: 'error' | 'success';
  onDismiss?: () => void;
}

export default function Toast({
  message,
  type = 'error',
  onDismiss,
}: ToastProps) {
  const bg = type === 'error' ? 'bg-red-50 border-red-200' : 'bg-green-50 border-green-200';
  const text = type === 'error' ? 'text-red-800' : 'text-green-800';

  return (
    <div
      className={`flex items-center justify-between px-4 py-3 rounded-lg border ${bg} ${text}`}
      role="alert"
    >
      <p className="text-sm font-medium">{message}</p>
      {onDismiss && (
        <button
          onClick={onDismiss}
          className="ml-4 p-1 rounded hover:bg-black/5 focus:outline-none"
          aria-label="Dismiss"
        >
          <svg className="w-4 h-4" fill="currentColor" viewBox="0 0 20 20">
            <path
              fillRule="evenodd"
              d="M4.293 4.293a1 1 0 011.414 0L10 8.586l4.293-4.293a1 1 0 111.414 1.414L11.414 10l4.293 4.293a1 1 0 01-1.414 1.414L10 11.414l-4.293 4.293a1 1 0 01-1.414-1.414L8.586 10 4.293 5.707a1 1 0 010-1.414z"
              clipRule="evenodd"
            />
          </svg>
        </button>
      )}
    </div>
  );
}
