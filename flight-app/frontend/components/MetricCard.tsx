const AIRCRAFT_NAMES: Record<string, string> = {
  '737': 'Boeing 737',
  '320': 'Airbus A320',
  'a320': 'Airbus A320',
  'boeing': 'Boeing 737',
  'airbus': 'Airbus A320',
};

interface MetricCardProps {
  title: string;
  value: string | number;
  subtitle?: string;
}

export default function MetricCard({ title, value, subtitle }: MetricCardProps) {
  const displayValue = title === 'Aircraft' && typeof value === 'string'
    ? (AIRCRAFT_NAMES[value?.toLowerCase()] ?? value)
    : value;
  return (
    <div className="bg-white rounded-xl shadow-sm p-6">
      <h3 className="text-sm font-medium text-gray-600">{title}</h3>
      <p className="text-2xl font-bold text-gray-900 mt-1">{displayValue}</p>
      {subtitle && (
        <p className="text-sm text-gray-500 mt-1">{subtitle}</p>
      )}
    </div>
  );
}
