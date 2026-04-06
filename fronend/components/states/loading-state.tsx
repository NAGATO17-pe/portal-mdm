type Props = {
  label?: string;
};

export const LoadingState = ({ label = "Cargando" }: Props) => (
  <div role="status" className="animate-pulse rounded-lg bg-white/5 p-4 text-sm">
    {label}...
  </div>
);
